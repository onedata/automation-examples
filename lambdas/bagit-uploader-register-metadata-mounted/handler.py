"""
Reads manifests from bagit archive and sets them as custom metadata for each file.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import abc
import contextlib
import hashlib
import os
import os.path
import tarfile
import traceback
import zipfile
from functools import lru_cache
from typing import IO, Final, Generator, List, NamedTuple, Optional, Set, Tuple

import xattr
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)
from typing_extensions import TypedDict

##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"

AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"adler32"}, hashlib.algorithms_available
)


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    archive: AtmFile
    destinationDir: AtmFile


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    pass


class Job(NamedTuple):
    heartbeat_callback: AtmHeartbeatCallback
    args: JobArgs


class BagitArchive(abc.ABC):
    def get_bagit_dir_name(self):
        if getattr(self, "_bagit_dir_name", None) is None:
            for file in self.list_files():
                path_tokens = file.split("/")
                if len(path_tokens) == 2 and path_tokens[1] == "bagit.txt":
                    self._bagit_dir_name = path_tokens[0]
                    break
            else:
                raise JobException("Bagit directory not found")

        return self._bagit_dir_name

    @abc.abstractmethod
    def build_file_path(self, file_rel_path: str, *, is_dir: bool = False) -> str:
        pass

    @abc.abstractmethod
    def list_files(self) -> List[str]:
        pass

    @abc.abstractmethod
    def open_file(self, path: str) -> IO[bytes]:
        pass


class ZipBagitArchive(BagitArchive):
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive

    def build_file_path(self, file_rel_path: str, *, is_dir=False) -> str:
        return f"{self.get_bagit_dir_name()}/{file_rel_path}{'/' if is_dir else ''}"

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.namelist()

    def open_file(self, path: str) -> IO[bytes]:
        return self.archive.open(path)


class TarBagitArchive(BagitArchive):
    def __init__(self, archive: tarfile.TarFile):
        self.archive = archive

    def build_file_path(self, file_rel_path: str, **kwargs) -> str:
        return f"{self.get_bagit_dir_name()}/{file_rel_path}"

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
) -> AtmJobBatchResponse[Optional[AtmException]]:

    job_results = []
    for job_args in job_batch_request["argsBatch"]:
        job = Job(args=job_args, heartbeat_callback=heartbeat_callback)
        job_results.append(run_job(job))

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Optional[AtmException]:
    try:
        if job.args["archive"]["type"] != "REG":
            return AtmException(exception=("Not an archive file"))

        with open_archive(job) as archive:
            process_manifest_files(job, archive)

    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return None


def process_manifest_files(job: Job, archive: BagitArchive) -> None:
    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        manifest_file = archive.build_file_path(f"manifest-{algorithm}.txt")
        if manifest_file in archive.list_files():
            process_manifest_file(job, archive, manifest_file, algorithm)


def process_manifest_file(
    job: Job, archive: BagitArchive, manifest_file: str, algorithm: str
) -> None:
    dst_dir_path = build_destination_dir_path(job)
    xattr_name = f"checksum.{algorithm}.expected"

    with archive.open_file(manifest_file) as fd:
        for line_num, line in enumerate(fd, start=1):
            job.heartbeat_callback()

            checksum, rel_file_path = parse_manifest_line(manifest_file, line_num, line)
            append_xattr(f"{dst_dir_path}/{rel_file_path}", xattr_name, checksum)


def parse_manifest_line(
    manifest_file: str, line_num: int, line: bytes
) -> Tuple[str, str]:
    try:
        checksum, file_path = line.decode("utf-8").strip().split()
        return checksum, os.path.relpath(file_path, "data/")
    except Exception:
        raise JobException(
            f"Failed to extract checksum and path from {manifest_file} line number {line_num}"
        )


def append_xattr(file_path: str, xattr_name: str, xattr_value: str) -> None:
    x = xattr.xattr(file_path)
    try:
        x.set(xattr_name, str.encode(f'"{xattr_value}"'))
    except Exception as ex:
        raise JobException(
            f"Failed to set xattr {xattr_name}:{xattr_value} on file {file_path} "
            f"due to: {str(ex)}"
        )


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'


def build_destination_dir_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["destinationDir"]["file_id"]}'
