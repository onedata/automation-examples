"""
A lambda which parses fetch.txt file (if it exists) in bagit archive and returns 
list of files to download.

NOTE: fetch file is a file where every line has format: <url>\s+<size>\s+<path>
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import abc
import contextlib
import os.path
import tarfile
import traceback
import zipfile
from functools import lru_cache
from typing import IO, Final, Generator, List, NamedTuple, Optional, Union

from typing_extensions import TypedDict

from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)

##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"


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


class JobResults(TypedDict):
    filesToDownload: List[FileDownloadInfo]
    statusLog: AtmObject


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    pass


class Job(NamedTuple):
    heartbeat_callback: AtmHeartbeatCallback
    args: JobArgs


class BagitArchive(abc.ABC):
    @abc.abstractmethod
    def list_files(self) -> List[str]:
        pass

    @abc.abstractmethod
    def open_file(self, path: str) -> IO[bytes]:
        pass

    def find_fetch_file(self) -> Optional[str]:
        all_files = self.list_files()

        for file in all_files:
            path_tokens = file.split("/")
            if len(path_tokens) == 2 and path_tokens[1] == "bagit.txt":
                fetch_file = f"{path_tokens[0]}/fetch.txt"
                return fetch_file if fetch_file in all_files else None

        return None


class ZipBagitArchive(BagitArchive):
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.namelist()

    def open_file(self, path: str) -> IO[bytes]:
        return self.archive.open(path)


class TarBagitArchive(BagitArchive):
    def __init__(self, archive: tarfile.TarFile):
        self.archive = archive

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.getnames()

    def open_file(self, path: str) -> IO[bytes]:
        fd = self.archive.extractfile(path)
        if fd is None:
            raise JobException(f"Couldn't open {path} in .tar archive")

        return fd


@contextlib.contextmanager
def open_archive(job: Job) -> Generator[BagitArchive, None, None]:
    archive_path = build_archive_path(job)
    _, archive_type = os.path.splitext(job.args["archive"]["name"])

    if archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            yield ZipBagitArchive(archive)
    elif archive_type == ".tar":
        with tarfile.TarFile(archive_path) as archive:
            yield TarBagitArchive(archive)
    elif archive_type in (".tgz", ".gz"):
        with tarfile.open(archive_path, "r:gz") as archive:
            yield TarBagitArchive(archive)
    else:
        raise JobException(f"Unsupported archive type: {archive_type}")


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:
    job_results = []
    for job_args in job_batch_request["argsBatch"]:
        job = Job(args=job_args, heartbeat_callback=heartbeat_callback)
        job_results.append(run_job(job))

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults]:
    try:
        with open_archive(job) as archive:
            files_to_download = parse_fetch_file(job, archive)
    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            "filesToDownload": files_to_download,
            "statusLog": {
                "severity": "info",
                "archive": job.args["archive"]["name"],
                "status": f"Found  {len(files_to_download)} files to be downloaded.",
            },
        }


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'


def parse_fetch_file(job: Job, archive: BagitArchive) -> List[FileDownloadInfo]:
    files_to_download = []

    if fetch_file := archive.find_fetch_file():
        dst_dir = f'.__onedata__file_id__{job.args["destinationDir"]["file_id"]}'

        for line_num, line in enumerate(archive.open_file(fetch_file), start=1):
            files_to_download.append(parse_line(dst_dir, line_num, line))
            job.heartbeat_callback()

    return files_to_download


def parse_line(dst_dir: str, line_num: int, line: bytes) -> FileDownloadInfo:
    try:
        decoded_line = line.decode("utf-8").strip("\n")
        url, size, rel_path = decoded_line.split(maxsplit=2)
        sanitized_size = int(size)
    except Exception:
        raise JobException(
            f"Failed to extract url, size and path from fetch file line number {line_num}"
        )
    else:
        if not rel_path.startswith("data/"):
            raise JobException(
                f"File path not within data/ directory (fetch.txt line {line_num})"
            )

        return {
            "sourceUrl": url,
            "destinationPath": f'{dst_dir}/{rel_path[len("data/"):]}',
            "size": sanitized_size,
        }
