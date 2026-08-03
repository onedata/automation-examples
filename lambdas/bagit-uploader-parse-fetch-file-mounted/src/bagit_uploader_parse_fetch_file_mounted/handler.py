"""
A lambda which parses a BagIt fetch.txt file and returns files to download.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import TypedDict

from onedata_lambda_utils import (
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    per_job,
)

from bagit_archive import (
    BagitArchive,
    extract_safe_data_relative_path,
    open_mounted_archive,
)


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    archive: AtmFile
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
    archive = job.args["archive"]
    if archive["type"] != "REG":
        raise JobException("Archive must be a regular file")

    with open_mounted_archive(archive) as bagit_archive:
        files_to_download = _parse_fetch_file(job.args, bagit_archive)

    return {
        "filesToDownload": files_to_download,
        "statusLog": {
            "severity": "info",
            "archive": archive["name"],
            "status": f"Found  {len(files_to_download)} files to be downloaded.",
        },
    }


def _parse_fetch_file(job_args: JobArgs, archive: BagitArchive) -> list[FileDownloadInfo]:
    if not (fetch_file := archive.find_fetch_file()):
        return []

    dst_dir = f".__onedata__file_id__{job_args['destinationDir']['fileId']}"
    with archive.open_file(fetch_file) as fd:
        return [_parse_line(dst_dir, line_num, line) for line_num, line in enumerate(fd, start=1)]


def _parse_line(dst_dir: str, line_num: int, line: bytes) -> FileDownloadInfo:
    try:
        decoded_line = line.decode("utf-8").strip("\n")
        url, size, rel_path = decoded_line.split(maxsplit=2)
        sanitized_size = int(size)
    except Exception as ex:
        raise JobException(
            f"Failed to extract url, size and path from fetch file line number {line_num}"
        ) from ex

    data_rel_path = extract_safe_data_relative_path(rel_path)

    return {
        "sourceUrl": url,
        "destinationPath": f"{dst_dir}/{data_rel_path}",
        "size": sanitized_size,
    }
