"""
A lambda which calculates checksums for file, and compares them with checksums from manifests, 
which were previously set as custom metadata under 'checksum.<algorithm>.expected' key.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import hashlib
import os.path
import queue
import traceback
import zlib
from threading import Event, Thread
from typing import IO, Final, List, NamedTuple, Optional, Set, Union

import xattr
from onedata_lambda_utils.stats import AtmTimeSeriesMeasurementBuilder
from onedata_lambda_utils.streaming import AtmResultStreamer
from onedata_lambda_utils.types import (
    AtmException,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmObject,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import TypedDict

##===================================================================
## Lambda configuration
##===================================================================


AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"adler32"}, hashlib.algorithms_available
)
READ_CHUNK_SIZE: Final[int] = 10 * 1024**2

##===================================================================
## Lambda interface
##===================================================================


STATS_STREAMER: Final[AtmResultStreamer[AtmTimeSeriesMeasurement]] = AtmResultStreamer(
    result_name="stats", synchronized=False
)


class FilesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="filesProcessed", unit=None
):
    pass


class BytesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"
):
    pass


class JobArgs(TypedDict):
    checksumAlgorithms: List[str]
    filePath: str


class JobResults(TypedDict):
    checksums: AtmObject


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    pass


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    jobs = [
        Job(args=job_args, ctx=job_batch_request["ctx"])
        for job_args in job_batch_request["argsBatch"]
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        job_results = list(executor.map(run_job, jobs))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults]:
    # Report 0 bytesProcessed to signal that thread is alive and running
    # so that heartbeat can be sent (needed in case when below functions
    # do not stream any measurements due to e.g. empty data directory)
    _measurements_queue.put(BytesProcessed.build(value=0))

    file_path = job.args["filePath"]
    file_info = {}
    file_info["file"] = file_path
    try:
        if not os.path.isfile(file_path):
            return {"checksums": file_info}

        xattr_file = xattr.xattr(file_path)

        for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
            expected_checksum_key = f"checksum.{algorithm}.expected"
            calculated_checksum_key = f"checksum.{algorithm}.calculated"
            if not expected_checksum_key in xattr_file.list():
                continue

            try:
                calculated_checksum = calculate_checksum(file_path, algorithm)
            except Exception as ex:
                raise JobException(
                    f"Failed to open file and calculate checksum due to: {str(ex)}"
                )

            try:
                expected_checksum = xattr_file.get(expected_checksum_key).decode("utf8")
                xattr_file.set(
                    calculated_checksum_key, str.encode(f'"{calculated_checksum}"')
                )
            except Exception as ex:
                raise JobException(
                    f"Failed set calculated checksum metadata due to: {str(ex)}"
                )

            _measurements_queue.put(FilesProcessed.build(value=1))

            if expected_checksum != calculated_checksum:
                raise JobException(
                    f"Expected file checksum: {expected_checksum}, when calculated checksum is: {calculated_checksum}"
                )

            file_info[algorithm] = {
                "expected": expected_checksum,
                "calculated": calculated_checksum,
                "status": "ok",
            }
    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"checksums": file_info}


def calculate_checksum(file_path: str, algorithm: str) -> str:
    with open(file_path, "rb") as file:
        if algorithm == "adler32":
            value = 1
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                value = zlib.adler32(data, value)
                _measurements_queue.put(BytesProcessed.build(value=len(data)))

            value_str = format(value, "x")
            return value_str.zfill(8)
        else:
            hash = getattr(hashlib, algorithm)()
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                hash.update(data)
                _measurements_queue.put(BytesProcessed.build(value=len(data)))
            return hash.hexdigest()


def monitor_jobs(heartbeat_callback: AtmHeartbeatCallback) -> None:
    any_job_ongoing = True
    while any_job_ongoing:
        any_job_ongoing = not _all_jobs_processed.wait(timeout=1)

        measurements = []
        while not _measurements_queue.empty():
            measurements.append(_measurements_queue.get())

        if measurements:
            STATS_STREAMER.stream_items(measurements)
            heartbeat_callback()
