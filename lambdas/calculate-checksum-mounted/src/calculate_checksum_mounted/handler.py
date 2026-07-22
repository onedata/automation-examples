"""
A lambda which calculates (and saves as metadata) file checksum using mounted Oneclient.

NOTE: This lambda works on any type of file by simply returning `None`
as checksum for anything but regular files.
"""

__author__ = "Rafał Widziszewski, Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
from typing import Final, TypedDict

import xattr
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    Job,
    JobContext,
    JobException,
    TimeSeriesMeasurementBuilder,
    mounted_file_path,
    per_job,
)

from checksum import ChecksumAlgorithm, assert_supported, calculate_checksum


##===================================================================
## Lambda configuration
##===================================================================


READ_CHUNK_SIZE: Final[int] = 10 * 1024**2


##===================================================================
## Lambda interface
##===================================================================


#: Result stream name for the time-series measurements (declare it in the lambda schema).
STATS_STREAM: Final[str] = "stats"


class FilesProcessed(TimeSeriesMeasurementBuilder, ts_name="filesProcessed", unit=None):
    pass


class BytesProcessed(TimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"):
    pass


class TaskConfig(TypedDict):
    algorithm: ChecksumAlgorithm
    metadataKey: str


class JobArgs(TypedDict):
    file: AtmFile


class FileChecksumReport(TypedDict):
    fileId: str
    algorithm: str
    checksum: str | None


class JobResult(TypedDict):
    result: FileChecksumReport


##===================================================================
## Lambda implementation
##===================================================================


@per_job(
    max_workers=DEFAULT_MAX_WORKERS,
    precondition=lambda ctx: assert_supported(ctx.config["algorithm"]),
)
def handle(job: Job[JobArgs], ctx: JobContext[TaskConfig]) -> JobResult:
    """
    Checksum one file. The algorithm is validated once per batch (the `precondition`), so an
    unsupported one fails the whole batch cleanly. Raising here instead fails only this job
    -- the SDK turns the exception into an `AtmException` entry (a `JobException` keeps just
    its message; any other exception carries a traceback).
    """
    algorithm = ctx.config["algorithm"]
    stats = ctx.result_streamer(STATS_STREAM)
    file_id = job.args["file"]["fileId"]
    file_path = mounted_file_path(file_id)

    if not os.path.isfile(file_path):
        # Non-regular file (dir, symlink, missing): no checksum, and not counted as processed.
        return {"result": {"fileId": file_id, "algorithm": algorithm, "checksum": None}}

    try:
        with open(file_path, "rb") as file:
            chunks = iter(lambda: file.read(READ_CHUNK_SIZE), b"")
            checksum = calculate_checksum(
                algorithm,
                chunks,
                on_bytes=lambda n: stats.stream_item(BytesProcessed.build(value=n)),
            )
        if metadata_key := ctx.config["metadataKey"]:
            _store_checksum_as_xattr(file_path, metadata_key, checksum)
        return {"result": {"fileId": file_id, "algorithm": algorithm, "checksum": checksum}}
    finally:
        # Count a regular file as processed even if checksumming/xattr failed (matches v2).
        stats.stream_item(FilesProcessed.build(value=1))


def _store_checksum_as_xattr(file_path: str, xattr_name: str, checksum: str) -> None:
    try:
        xattr.xattr(file_path).set(xattr_name, checksum.encode())
    except OSError as ex:
        raise JobException(f"Failed to set xattr {xattr_name!r} on the file: {ex}") from ex
