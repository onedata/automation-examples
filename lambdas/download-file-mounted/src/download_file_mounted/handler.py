"""A lambda which downloads files."""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


from collections.abc import Iterator
from pathlib import Path
from typing import Final, TypedDict

import requests
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmObject,
    Job,
    JobContext,
    JobException,
    TimeSeriesMeasurementBuilder,
    mount_point,
    per_job,
)
from onedata_lambda_utils.logging import Logger
from onedata_lambda_utils.streaming import ResultStreamer


##===================================================================
## Lambda configuration
##===================================================================


DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2

EXTENDED_REST_REQUEST_TIMEOUT: Final[int] = 120


##===================================================================
## Lambda interface
##===================================================================


STATS_STREAM: Final[str] = "stats"

LOGS_STREAM: Final[str] = "logs"

USER_AGENT: Final[str] = "onedata-download-file-mounted/4.0"


class FilesProcessed(TimeSeriesMeasurementBuilder, ts_name="filesProcessed", unit=None):
    pass


class BytesProcessed(TimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"):
    pass


class FileDownloadInfo(TypedDict):
    sourceUrl: str
    destinationPath: str
    size: int


class JobArgs(TypedDict):
    downloadInfo: FileDownloadInfo


class JobResult(TypedDict):
    processedFilePath: str


##===================================================================
## Lambda implementation
##===================================================================


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    stats = ctx.result_streamer(STATS_STREAM)
    log = ctx.logger(LOGS_STREAM)
    try:
        _run_job(job.args, log, stats)
    finally:
        stats.stream_item(FilesProcessed.build(value=1))
    return {"processedFilePath": job.args["downloadInfo"]["destinationPath"]}


def _run_job(job_args: JobArgs, log: Logger, stats: ResultStreamer) -> None:
    destination_path = _build_destination_path(job_args)

    if destination_path.exists():
        if destination_path.stat().st_size == job_args["downloadInfo"]["size"]:
            log.info(
                {
                    "downloadInfo": job_args["downloadInfo"],
                    "message": (
                        "Skipping download as file with expected size "
                        "already exists at destination path."
                    ),
                }
            )
        else:
            log.info(
                {
                    "downloadInfo": job_args["downloadInfo"],
                    "message": (
                        "Removing file existing at destination path "
                        "(probably artefact of previous failed download).",
                    ),
                }
            )
            destination_path.unlink()
            _download_file(job_args, stats)
    else:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        _download_file(job_args, stats)


def _download_file(job_args: JobArgs, stats: ResultStreamer) -> None:
    if job_args["downloadInfo"]["sourceUrl"].startswith("root:/"):
        _download_xrootd_file(job_args, stats)
    else:
        _download_http_file(job_args, stats)


def _download_xrootd_file(job_args: JobArgs, stats: ResultStreamer) -> None:
    from XRootD import client  # noqa: PLC0415
    from XRootD.client.flags import OpenFlags  # noqa: PLC0415

    url = job_args["downloadInfo"]["sourceUrl"]

    with client.File() as fd:
        status, _ = fd.open(url, OpenFlags.READ)
        if not status.ok:
            raise JobException(f"Failed to open xrootd file at {url} due to: {status.message}")

        data_stream = fd.readchunks(offset=0, chunksize=DOWNLOAD_CHUNK_SIZE)

        _write_file(job_args, data_stream, stats)


def _download_http_file(job_args: JobArgs, stats: ResultStreamer) -> None:
    try:
        request = requests.get(
            job_args["downloadInfo"]["sourceUrl"],
            # some websites won't allow downloads without the user-agent header
            headers={"user-agent": USER_AGENT},
            stream=True,
            allow_redirects=True,
            timeout=EXTENDED_REST_REQUEST_TIMEOUT,
        )
        request.raise_for_status()
    except requests.RequestException as ex:
        raise JobException(f"HTTP download failed: {ex}") from ex

    _write_file(job_args, request.iter_content(DOWNLOAD_CHUNK_SIZE), stats)


def _write_file(job_args: JobArgs, data_stream: Iterator[bytes], stats: ResultStreamer) -> None:
    file_size = 0
    destination_path = _build_destination_path(job_args)
    with open(destination_path, "wb") as f:
        for chunk in data_stream:
            bytes_written = f.write(chunk)
            chunk_size = len(chunk)
            if bytes_written != chunk_size:
                raise JobException(
                    "Unable to write a data chunk to file; "
                    f"written {bytes_written} bytes instead of {chunk_size}."
                )

            file_size += chunk_size
            stats.stream_item(BytesProcessed.build(value=chunk_size))

    if file_size != job_args["downloadInfo"]["size"]:
        raise JobException(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({file_size} B) size of download"
        )

    # serves as a double check that the Oneclient mounted as a sidecar has
    # coherent information about the file size after all chunks are written
    actual_size = destination_path.stat().st_size
    if actual_size != job_args["downloadInfo"]["size"]:
        raise JobException(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({actual_size} B) size of the file "
            "stored in the target location."
        )


def _build_destination_path(job_args: JobArgs) -> Path:
    destination_path = Path(job_args["downloadInfo"]["destinationPath"])
    if destination_path.is_absolute():
        raise JobException("Destination path must be relative")

    mount = Path(mount_point()).resolve()
    resolved_path = (mount / destination_path).resolve()
    if not resolved_path.is_relative_to(mount):
        raise JobException("Destination path must stay within the Oneclient mount point")

    return resolved_path
