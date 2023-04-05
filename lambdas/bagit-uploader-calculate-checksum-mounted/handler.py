"""
A lambda which calculates checksums for file, and compares them with checksums from manifests, 
which were previously set as custom metadata under 'checksum.<algorithm>.expected' key.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import hashlib
import queue
import re
import time
import traceback
import zlib
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from threading import Event, Thread
from typing import (
    Dict,
    Final,
    List,
    Literal,
    NamedTuple,
    Tuple,
    TypeAlias,
    Union,
    get_args,
)

import xattr
from onedata_lambda_utils.streaming import AtmResultStreamer
from onedata_lambda_utils.types import (
    AtmException,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import TypedDict

##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"

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


ChecksumAlgorithm: TypeAlias = Literal[
    "adler32",
    "blake2b",
    "blake2s",
    "md5",
    "sha1",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "sha3_224",
    "sha3_256",
    "sha3_384",
    "sha3_512",
    "shake_128",
    "shake_256",
]


class ChecksumStatus(TypedDict):
    expected: str
    calculated: str
    status: str


ChecksumReport: TypeAlias = Dict[ChecksumAlgorithm, ChecksumStatus]


class JobChecksumsReport(TypedDict):
    filePath: str
    checksums: ChecksumReport


class JobResults(TypedDict):
    result: JobChecksumsReport


##===================================================================
## Lambda implementation
##===================================================================


RE_EXP_CHECKSUM_XATTR_NAME: Final[
    str
] = r"^checksum.(?P<algorithm>{algorithms}).expected$".format(
    algorithms="|".join(get_args(ChecksumAlgorithm))
)


class JobException(Exception):
    pass


class ExpFileChecksum(NamedTuple):
    file_path: str
    algorithm: ChecksumAlgorithm
    checksum: str


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    with ThreadPoolExecutor() as executor:
        jobs_results_or_futures = []

        for job_args in job_batch_request["argsBatch"]:
            file_path = build_file_path(job_args)

            if futures := schedule_file_verifications(executor, file_path):
                jobs_results_or_futures.append((file_path, futures))
            else:
                jobs_results_or_futures.append(build_job_results(file_path, {}))

        results_batch = list(map(assemble_results, jobs_results_or_futures))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": results_batch}


def build_file_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/{job_args["filePath"]}'


def schedule_file_verifications(executor: Executor, file_path: str) -> List[Future]:
    futures = []

    file_xattrs = xattr.xattr(file_path)
    for xattr_name in file_xattrs.list():
        if match := re.match(RE_EXP_CHECKSUM_XATTR_NAME, xattr_name):
            futures.append(
                executor.submit(
                    verify_file_checksum,
                    file_path,
                    match.group("algorithm"),
                    file_xattrs.get(xattr_name).decode("utf8"),
                )
            )

    return futures


def assemble_results(
    job_results_or_futures: Union[JobResults, Tuple[str, List[Future]]]
) -> Union[JobResults, AtmException]:
    match job_results_or_futures:
        case {"result": _}:
            return job_results_or_futures

        case (file_path, futures):
            checksums = {}
            for future in futures:
                verification_result = future.result()
                if "exception" in verification_result:
                    return verification_result

                checksums.update(verification_result)
            else:
                return build_job_results(file_path, checksums)


def build_job_results(file_path: str, checksums: ChecksumReport) -> JobResults:
    return {"result": {"filePath": file_path, "checksums": checksums}}


def verify_file_checksum(
    file_path: str, algorithm: ChecksumAlgorithm, exp_checksum: str
) -> Union[AtmException, ChecksumReport]:
    try:
        checksum = calculate_checksum(file_path, algorithm)
        set_file_xattr(file_path, f"checksum.{algorithm}.calculated", checksum)
        assert_exp_checksum(checksum, exp_checksum)
    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            algorithm: {
                "expected": exp_checksum,
                "calculated": checksum,
                "status": "ok",
            }
        }


def calculate_checksum(file_path: str, algorithm: ChecksumAlgorithm) -> str:
    try:
        return calculate_checksum_insecure(file_path, algorithm)
    except Exception as ex:
        raise JobException(f"Failed to calculate checksum due to: {str(ex)}")


def calculate_checksum_insecure(file_path: str, algorithm: ChecksumAlgorithm) -> str:
    with open(file_path, "rb") as file:
        if algorithm == "adler32":
            value = 1
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                value = zlib.adler32(data, value)
                _measurements_queue.put(build_time_series_measurement(algorithm, data))
            return format(value, "x")
        else:
            hash = getattr(hashlib, algorithm)()
            for data in iter(lambda: file.read(READ_CHUNK_SIZE), b""):
                hash.update(data)
                _measurements_queue.put(build_time_series_measurement(algorithm, data))
            return hash.hexdigest()


def build_time_series_measurement(
    algorithm: ChecksumAlgorithm, data: bytes
) -> AtmTimeSeriesMeasurement:
    return {
        "tsName": f"bytesProcessed_{algorithm}",
        "timestamp": int(time.time()),
        "value": len(data),
    }


def set_file_xattr(file_path: str, xattr_name: str, xattr_value: str) -> None:
    file_xattrs = xattr.xattr(file_path)

    try:
        file_xattrs.set(xattr_name, str.encode(f'"{xattr_value}"'))
    except Exception as ex:
        raise JobException(
            f"Failed to set xattr {xattr_name}:{xattr_value} due to: {str(ex)}"
        )


def assert_exp_checksum(checksum: str, exp_checksum: str) -> None:
    if checksum != exp_checksum:
        raise JobException(
            f"Expected file checksum: {exp_checksum}, when calculated checksum is: {checksum}"
        )


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
