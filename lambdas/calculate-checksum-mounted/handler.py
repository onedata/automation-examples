"""
A lambda which calculates (and saves as metadata) file checksum using mounted Oneclient.

NOTE: This lambda works on any type of file by simply returning `None` 
as checksum for anything but regular files.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import hashlib
import os
import queue
import traceback
import zlib
from threading import Event, Thread
from typing import Final, NamedTuple, Optional, Set, Union

import xattr
from onedata_lambda_utils.stats import AtmTimeSeriesMeasurementBuilder
from onedata_lambda_utils.streaming import AtmResultStreamer
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import TypedDict


##===================================================================
## Lambda configuration
##===================================================================

MOUNT_POINT: Final[str] = "/mnt/onedata"

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
    file: AtmFile
    algorithm: str
    metadataKey: str


class FileChecksumReport(TypedDict):
    file_id: str
    algorithm: str
    checksum: Optional[str]


class JobResults(TypedDict):
    result: FileChecksumReport


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
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
    file_path = build_file_path(job)

    if not os.path.isfile(file_path):
        return build_job_results(job, None)

    if job.args["algorithm"] not in AVAILABLE_CHECKSUM_ALGORITHMS:
        return AtmException(
            exception=(
                f"{job.args['algorithm']} algorithm is unsupported."
                f"Available ones are: {AVAILABLE_CHECKSUM_ALGORITHMS}"
            )
        )

    try:
        checksum = calculate_checksum(job, file_path)

        if job.args["metadataKey"] != "":
            set_file_metadata(job, file_path, checksum)

        _measurements_queue.put(FilesProcessed.build(value=1))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job, checksum)


def build_file_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["file"]["file_id"]}'


def build_job_results(job: Job, checksum: Optional[str]) -> JobResults:
    return {
        "result": {
            "file_id": job.args["file"]["file_id"],
            "algorithm": job.args["algorithm"],
            "checksum": checksum,
        }
    }


def calculate_checksum(job: Job, file_path: str) -> str:
    algorithm = job.args["algorithm"]

    with open(file_path, "rb") as file:
        if algorithm == "adler32":
            value = 1
            while True:
                data = file.read(READ_CHUNK_SIZE)
                if not data:
                    break
                value = zlib.adler32(data, value)
                _measurements_queue.put(BytesProcessed.build(value=len(data)))
            return format(value, "x")
        else:
            hash = getattr(hashlib, algorithm)()
            while True:
                data = file.read(READ_CHUNK_SIZE)
                if not data:
                    break
                hash.update(data)
                _measurements_queue.put(BytesProcessed.build(value=len(data)))
            return hash.hexdigest()


def set_file_metadata(job: Job, file_path: str, checksum: str) -> None:
    metadata = xattr.xattr(file_path)
    metadata.set(job.args["metadataKey"], str.encode(checksum))


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
