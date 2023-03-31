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
from typing import (
    Final,
    FrozenSet,
    Literal,
    NamedTuple,
    Optional,
    TypeAlias,
    Union,
    get_args,
)

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

AVAILABLE_CHECKSUM_ALGORITHMS: Final[FrozenSet[ChecksumAlgorithm]] = frozenset(
    get_args(ChecksumAlgorithm)
)


class TaskConfig(TypedDict):
    algorithm: ChecksumAlgorithm
    metadataKey: str


class JobArgs(TypedDict):
    file: AtmFile


class FileChecksumReport(TypedDict):
    file_id: str
    algorithm: str
    checksum: Optional[str]


class JobResults(TypedDict):
    result: FileChecksumReport


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    pass


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx[TaskConfig]
    args: JobArgs


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, TaskConfig],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    algorithm = job_batch_request["ctx"]["config"]["algorithm"]
    if algorithm not in AVAILABLE_CHECKSUM_ALGORITHMS:
        return AtmException(
            exception=(
                f"{algorithm} algorithm is unsupported. "
                f"Available ones are: {AVAILABLE_CHECKSUM_ALGORITHMS}"
            )
        )

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

    try:
        algorithm = job.ctx["config"]["algorithm"]
        checksum = calculate_checksum(algorithm, file_path)

        if xattr_name := job.ctx["config"]["metadataKey"]:
            set_file_checksum_xattr(file_path, xattr_name, checksum)
    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job, checksum)
    finally:
        _measurements_queue.put(FilesProcessed.build(value=1))


def build_file_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["file"]["file_id"]}'


def build_job_results(job: Job, checksum: Optional[str]) -> JobResults:
    return {
        "result": {
            "file_id": job.args["file"]["file_id"],
            "algorithm": job.ctx["config"]["algorithm"],
            "checksum": checksum,
        }
    }


def calculate_checksum(algorithm: ChecksumAlgorithm, file_path: str) -> str:
    with open(file_path, "rb") as fd:
        data_stream = iter(lambda: fd.read(READ_CHUNK_SIZE), b"")

        if algorithm == "adler32":
            value = 1
            for data in data_stream:
                value = zlib.adler32(data, value)
                _measurements_queue.put(BytesProcessed.build(value=len(data)))
            return format(value, "x")
        else:
            hash = getattr(hashlib, algorithm)()
            for data in data_stream:
                hash.update(data)
                _measurements_queue.put(BytesProcessed.build(value=len(data)))
            return hash.hexdigest()


def set_file_checksum_xattr(file_path: str, xattr_name: str, checksum: str) -> None:
    file_xattrs = xattr.xattr(file_path)

    try:
        file_xattrs.set(xattr_name, str.encode(checksum))
    except Exception as ex:
        raise JobException(
            f"Failed to set xattr {xattr_name}:{checksum} due to: {str(ex)}"
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
