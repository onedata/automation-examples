"""
A lambda which parses fetch files and returns list of files to download.

Each fetch file line must have format: <url> <size> <path>.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path, PurePosixPath
from typing import TypedDict

from onedata_lambda_utils import (
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    mounted_file_path,
    per_job,
)


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    fetchFile: AtmFile
    destinationDir: AtmFile


class FileDownloadInfo(TypedDict):
    sourceUrl: str
    destinationPath: str
    size: int


class JobResult(TypedDict):
    filesToDownload: list[FileDownloadInfo]
    statusLog: AtmObject


##===================================================================
## Lambda implementation
##===================================================================


@per_job
def handle(job: Job[JobArgs], _ctx: JobContext[AtmObject]) -> JobResult:
    fetch_file = job.args["fetchFile"]

    if fetch_file["type"] == "DIR":
        files_to_download: list[FileDownloadInfo] = []
    elif fetch_file["type"] != "REG":
        raise JobException("Fetch file must be a regular file or directory")
    else:
        files_to_download = _parse_fetch_file(job.args)

    return {
        "filesToDownload": files_to_download,
        "statusLog": {
            "severity": "info",
            "fetchFileName": fetch_file["name"],
            "status": f"Found  {len(files_to_download)} files to be downloaded.",
        },
    }


def _parse_fetch_file(job_args: JobArgs) -> list[FileDownloadInfo]:
    fetch_file_path = Path(mounted_file_path(job_args["fetchFile"]["fileId"]))
    dst_dir = f".__onedata__file_id__{job_args['destinationDir']['fileId']}"

    if fetch_file_path.is_dir():
        return []

    with open(fetch_file_path) as file:
        return [_parse_line(dst_dir, line_num, line) for line_num, line in enumerate(file, start=1)]


def _parse_line(dst_dir: str, line_num: int, line: str) -> FileDownloadInfo:
    try:
        url, size, rel_path = line.strip().split(maxsplit=2)
        sanitized_size = int(size)
    except Exception as ex:
        raise JobException(
            f"Failed to extract url, size and path from fetch file line number {line_num}"
        ) from ex

    destination_rel_path = _sanitize_destination_path(rel_path, line_num)
    return {
        "sourceUrl": url,
        "destinationPath": f"{dst_dir}/{destination_rel_path}",
        "size": sanitized_size,
    }


def _sanitize_destination_path(rel_path: str, line_num: int) -> PurePosixPath:
    path = PurePosixPath(rel_path.lstrip("/"))
    if not path.parts or path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise JobException(f"Unsafe destination path in fetch file line number {line_num}")
    return path
