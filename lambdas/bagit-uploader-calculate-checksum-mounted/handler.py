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
import itertools
import time
import traceback
import zlib
import re
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, as_completed, wait
from threading import Event, Thread
from typing import (
    IO,
    Dict,
    Final,
    Generator,
    List,
    Literal,
    NamedTuple,
    Set,
    Tuple,
    TypeAlias,
    Union,
)

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

## Statistics are streamed as the file is processed during checksum counting in form of "bytesProcessed__{algorithm}",
## where algorithm is defined by how the checksum is calculated.

STATS_STREAMER: Final[AtmResultStreamer[AtmTimeSeriesMeasurement]] = AtmResultStreamer(
    result_name="stats", synchronized=False
)


class JobArgs(TypedDict):
    filePath: str


class ChecksumInfo(TypedDict):
    expected: str
    calculated: str
    status: str


ChecksumAlgorithm: TypeAlias = Literal[
    "sha1",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "blake2b",
    "blake2s",
    "md5",
    "sha3_224",
    "sha3_256",
    "sha3_384",
    "sha3_512",
    "shake_128",
    "shake_256",
]

ChecksumsInfoMap = Dict[ChecksumAlgorithm, ChecksumInfo]


class JobHeh(TypedDict):
    checksums: ChecksumsInfoMap
    filePath: str


class JobResults(TypedDict):
    result: JobHeh


##===================================================================
## Lambda implementation
##===================================================================

MATCH_CHECKSUM_ALGORITHM: Final[str] = "^checksum.(?P<algorithm>.+).expected$"


class JobException(Exception):
    pass


class ExpFileChecksum(NamedTuple):
    file_path: str
    file_info: dict


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    tasks = get_exp_file_checksums(job_batch_request)

    with ThreadPoolExecutor() as executor:
        jobs_executed = list(executor.map(verify_file_checksum, tasks))

    results_batch = assemble_results(tasks, jobs_executed)

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": results_batch}


def get_exp_file_checksums(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject]
) -> List[Tuple[str, str, str]]:
    jobs = []

    for job_args in job_batch_request["argsBatch"]:
        file_path = build_file_path(job_args)
        xattr_file = xattr.xattr(file_path)

        if os.path.isfile(file_path):
            for xattr_content in xattr_file.list():
                match = re.match(MATCH_CHECKSUM_ALGORITHM, xattr_content)
                if match:
                    algorithm = match.group("algorithm")
                    expected_checksum_key = f"checksum.{algorithm}.expected"
                    expected_checksum = xattr_file.get(expected_checksum_key).decode(
                        "utf8"
                    )
                    jobs.append((file_path, algorithm, expected_checksum))

    return jobs


def assemble_results(
    tasks: List[Tuple[str, str, str]],
    results: List[Union[AtmException, ExpFileChecksum]],
) -> Union[AtmException, JobResults]:
    file_paths = []
    file_paths_with_exception = []
    file_paths_appended = []

    results_dict = dict()
    results_batch = []

    for task in tasks:
        file_path, _, _ = task
        file_paths.append(file_path)

    for (file_path, expFileChecksum) in zip(file_paths, results):
        if not hasattr(expFileChecksum, "file_info"):
            file_paths_with_exception.append(file_path)

    for (file_path, expFileChecksum) in zip(file_paths, results):
        if (
            hasattr(expFileChecksum, "file_path")
            and not expFileChecksum.file_path in file_paths_with_exception
        ):
            results_dict.setdefault(expFileChecksum.file_path, {}).update(
                expFileChecksum.file_info
            )
        else:
            if not file_path in file_paths_appended:
                results_batch.append(expFileChecksum["exception"])
                file_paths_appended.append(file_path)

    results_batch.append(
        {"result": {"checksums": results_dict[key], "filePath": key}}
        for key in results_dict
    )

    return results_batch


def verify_file_checksum(
    job: Tuple[str, str, str]
) -> Union[JobException, ExpFileChecksum]:
    file_path, algorithm, expected_checksum = job
    file_info = {}

    try:
        file_info[algorithm] = calculate_and_compare_checksums(
            algorithm, file_path, expected_checksum
        )

    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return ExpFileChecksum(file_path, file_info)


def calculate_and_compare_checksums(
    algorithm: str, file_path: str, expected_checksum: str
) -> Union[AtmException, None, dict]:
    xattr_file = xattr.xattr(file_path)

    calculated_checksum_key = f"checksum.{algorithm}.calculated"

    calculated_checksum = calculate_file_checksum(file_path, algorithm)

    set_calculated_checksum_metadata(
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
    calculated_checksum_key: str,
    calculated_checksum: str,
    xattr_file: xattr,
) -> Union[None, AtmException]:
    try:
        xattr_file.set(
            calculated_checksum_key,
            str.encode(f'"{calculated_checksum}"'),
        )
    except Exception as ex:
        raise JobException(f"Failed set calculated checksum metadata due to: {str(ex)}")


def calculate_file_checksum(file_path: str, algorithm: str) -> Union[str, AtmException]:
    try:
        return calculate_checksum(file_path, algorithm)
    except Exception as ex:
        raise JobException(f"Failed to calculate checksum due to: {str(ex)}")


def calculate_checksum(file_path: str, algorithm: str) -> str:
    with open(file_path, "rb") as file:
        if algorithm == "adler32":
            value = 1
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                value = zlib.adler32(data, value)
                _measurements_queue.put(build_time_series_measurment(algorithm, data))

            value_str = format(value, "x")
            return value_str
        else:
            hash = getattr(hashlib, algorithm)()
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                hash.update(data)
                _measurements_queue.put(build_time_series_measurment(algorithm, data))
            return hash.hexdigest()


def build_time_series_measurment(algorithm: str, data: bytes):
    return {
        "tsName": f"bytesProcessed_{algorithm}",
        "timestamp": int(time.time()),
        "value": len(data),
    }


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
