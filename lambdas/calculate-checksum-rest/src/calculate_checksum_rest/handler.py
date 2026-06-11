"""
A lambda which calculates (and saves as metadata) file checksum using REST interface.

NOTE: This lambda works on any type of file by simply returning `None`
as checksum for anything but regular files.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
from collections.abc import Iterator
from typing import Final, TypedDict

import requests
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    Job,
    JobContext,
    JobException,
    TimeSeriesMeasurementBuilder,
    per_job,
)

from checksum import ChecksumAlgorithm, assert_supported, calculate_checksum


##===================================================================
## Lambda configuration
##===================================================================


DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2
REST_REQUEST_TIMEOUT: Final[int] = 60
EXTENDED_REST_REQUEST_TIMEOUT: Final[int] = 120

#: Read at call time (testability): SSL verification is on unless explicitly disabled.
ENV_VERIFY_SSL: Final[str] = "VERIFY_SSL_CERTIFICATES"


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
    Checksum one file via REST. The algorithm is validated once per batch (the
    `precondition`), so an unsupported one fails the whole batch cleanly. Raising here
    instead fails only this job -- the SDK turns the exception into an `AtmException` entry
    (a `JobException` keeps just its message; any other exception carries a traceback).
    """
    algorithm = ctx.config["algorithm"]
    stats = ctx.result_streamer(STATS_STREAM)
    file = job.args["file"]
    file_id = file["fileId"]

    if file["type"] != "REG":
        # Only regular files have content to checksum; non-regular ones are not counted.
        return {"result": {"fileId": file_id, "algorithm": algorithm, "checksum": None}}

    try:
        content = _stream_file_content(ctx, file_id)
        checksum = calculate_checksum(
            algorithm,
            content,
            on_bytes=lambda n: stats.stream_item(BytesProcessed.build(value=n)),
        )
        if metadata_key := ctx.config["metadataKey"]:
            _store_checksum_as_xattr(ctx, file_id, metadata_key, checksum)
        return {"result": {"fileId": file_id, "algorithm": algorithm, "checksum": checksum}}
    except requests.RequestException as ex:
        # A known transport failure -- fail just this job with a clean message.
        raise JobException(f"REST request failed: {ex}") from ex
    finally:
        # Count a regular file as processed even if the request failed (matches v2).
        stats.stream_item(FilesProcessed.build(value=1))


def _stream_file_content(ctx: JobContext[TaskConfig], file_id: str) -> Iterator[bytes]:
    response = requests.get(
        _build_file_rest_url(ctx, file_id, "content"),
        headers={"x-auth-token": ctx.access_token},
        stream=True,
        verify=_verify_ssl(),
        timeout=EXTENDED_REST_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE)


def _store_checksum_as_xattr(
    ctx: JobContext[TaskConfig], file_id: str, xattr_name: str, checksum: str
) -> None:
    response = requests.put(
        _build_file_rest_url(ctx, file_id, "metadata/xattrs"),
        headers={"x-auth-token": ctx.access_token, "content-type": "application/json"},
        json={xattr_name: checksum},
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )
    response.raise_for_status()


def _build_file_rest_url(ctx: JobContext[TaskConfig], file_id: str, subpath: str) -> str:
    domain = ctx.oneprovider_domain
    return f"https://{domain}/api/v3/oneprovider/data/{file_id}/{subpath.lstrip('/')}"


def _verify_ssl() -> bool:
    return os.environ.get(ENV_VERIFY_SSL) != "false"
