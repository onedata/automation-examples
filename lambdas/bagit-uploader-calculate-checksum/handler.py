"""
A lambda which calculates checksums for file, and compares them with checksums from manifests, 
which were previously set as custom metadata under 'checksum.<algorithm>.expected' key.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import hashlib
import os.path
import queue
import time
import traceback
import zlib
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, as_completed, wait
from threading import Event, Thread
from typing import IO, Final, Generator, List, NamedTuple, Set, Tuple, Union

import xattr
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


class JobArgs(TypedDict):
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

    jobs = list(generate_jobs_to_execute(job_batch_request))
    results = []

    with ThreadPoolExecutor() as executor:
        jobs_executed = [executor.submit(run_job, job) for job in jobs]
        wait(jobs_executed, return_when=ALL_COMPLETED)

    for job in as_completed(jobs_executed):
        result = job.result()
        results.append(result)

    job_results = generate_jobs_results(results)

    _all_jobs_processed.set()

    return {"resultsBatch": job_results}


def generate_jobs_results(results: List[Tuple[str, dict]]):
    results_dict = dict()
    job_results = []

    for file_path, file_info in results:
        results_dict.setdefault(file_path, {}).update(file_info)

    for key in results_dict:
        checksum_info = {"checksums": results_dict[key]}
        job_results.append(checksum_info)

    return job_results


def generate_jobs_to_execute(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject]
) -> Generator[Tuple, None, None]:

    for job_args in job_batch_request["argsBatch"]:
        file_path = build_file_path(job_args)
        algorithms = []

        if os.path.isfile(file_path):
            for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
                xattr_file = xattr.xattr(file_path)
                expected_checksum_key = f"checksum.{algorithm}.expected"
                if expected_checksum_key in xattr_file.list():
                    algorithms.append(algorithm)

        for algorithm in algorithms:
            yield (file_path, algorithm)


def run_job(job: Tuple[str, str]) -> Union[JobException, Tuple[str, dict]]:
    file_path, algorithm = job
    file_info = {}

    try:
        if not os.path.isfile(file_path):
            return file_path, file_info

        xattr_file = xattr.xattr(file_path)

        file_info[algorithm] = calculate_and_compare_checksums(
            algorithm, xattr_file, file_path
        )

    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return file_path, file_info


def calculate_and_compare_checksums(
    algorithm: str, xattr_file: xattr, file_path: str
) -> Union[AtmException, None, dict]:
    expected_checksum_key = f"checksum.{algorithm}.expected"
    calculated_checksum_key = f"checksum.{algorithm}.calculated"
    if not expected_checksum_key in xattr_file.list():
        return None

    calculated_checksum = get_file_checksum(file_path, algorithm)

    expected_checksum = set_calculated_checksum_metadata(
        expected_checksum_key,
        calculated_checksum_key,
        calculated_checksum,
        xattr_file,
    )

    if expected_checksum != calculated_checksum:
        raise JobException(
            f"Expected file checksum: {expected_checksum}, when calculated checksum is: {calculated_checksum}"
        )

    return {
        "expected": expected_checksum,
        "calculated": calculated_checksum,
        "status": "ok",
    }


def build_file_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/{job_args["filePath"]}'


def set_calculated_checksum_metadata(
    expected_checksum_key: str,
    calculated_checksum_key: str,
    calculated_checksum: str,
    xattr_file: xattr,
) -> Union[str, AtmException]:
    try:
        expected_checksum = xattr_file.get(expected_checksum_key).decode("utf8")
        xattr_file.set(
            calculated_checksum_key,
            str.encode(f'"{calculated_checksum}"'),
        )
    except Exception as ex:
        raise JobException(f"Failed set calculated checksum metadata due to: {str(ex)}")

    return expected_checksum


def get_file_checksum(file_path: str, algorithm: str) -> Union[str, AtmException]:
    try:
        return calculate_checksum(file_path, algorithm)
    except Exception as ex:
        raise JobException(
            f"Failed to open file and calculate checksum due to: {str(ex)}"
        )


def calculate_checksum(file_path: str, algorithm: str) -> str:
    with open(file_path, "rb") as file:
        if algorithm == "adler32":
            value = 1
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                value = zlib.adler32(data, value)
                _measurements_queue.put(
                    {
                        "tsName": f"bytesProcessed_{algorithm}",
                        "timestamp": int(time.time()),
                        "value": len(data),
                    }
                )

            value_str = format(value, "x")
            return value_str.zfill(8)
        else:
            hash = getattr(hashlib, algorithm)()
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                hash.update(data)
                _measurements_queue.put(
                    {
                        "tsName": f"bytesProcessed_{algorithm}",
                        "timestamp": int(time.time()),
                        "value": len(data),
                    }
                )
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
