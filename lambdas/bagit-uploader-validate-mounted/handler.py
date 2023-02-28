"""
A lambda which validates bagit archives.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import abc
import contextlib
import hashlib
import os
import os.path
import re
import tarfile
import traceback
import zipfile
import zlib
from functools import lru_cache
from typing import IO, Final, Generator, Iterator, List, NamedTuple, Set, Tuple

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

SUPPORTED_URL_SCHEMAS: Final[Tuple[str, ...]] = ("root:", "http:", "https:")

AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"adler32"}, hashlib.algorithms_available
)
READ_CHUNK_SIZE: Final[int] = 10 * 1024**2


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    archive: AtmFile


class JobResults(TypedDict):
    validArchives: List[AtmFile]
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

    @lru_cache
    def list_manifest_files(self) -> List[str]:
        manifests = []
        for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
            manifest_file = self.build_file_path(f"manifest-{algorithm}.txt")
            if manifest_file in self.list_files():
                manifests.append(manifest_file)

        return manifests

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
            raise JobException(f"Couldn't open {path} in archive")

        return fd


@contextlib.contextmanager
def open_archive(job_args: JobArgs) -> Generator[BagitArchive, None, None]:
    archive_path = build_archive_path(job_args)
    _, archive_type = os.path.splitext(job_args["archive"]["name"])

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
    results = []
    for job_args in job_batch_request["argsBatch"]:
        heartbeat_callback()
        results.append(run_job(job_args))

    return {"resultsBatch": results}


def run_job(job_args: JobArgs) -> JobResults:
    try:
        if job_args["archive"]["type"] != "REG":
            return AtmException(exception=("Not an archive file"))

        with open_archive(job_args) as archive:
            assert_valid_archive(archive)

    except JobException as ex:
        return {
            "validArchives": [],
            "statusLog": {
                "archive": job_args["archive"]["name"],
                "status": f"Invalid bagit archive",
                "reason": str(ex),
            },
        }
    except Exception:
        return {
            "validArchives": [],
            "statusLog": {
                "archive": job_args["archive"]["name"],
                "status": f"Failed to validate bagit archive",
                "reason": traceback.format_exc(),
            },
        }
    else:
        return {
            "validArchives": [job_args["archive"]],
            "statusLog": {
                "archive": job_args["archive"]["name"],
                "status": f"Valid bagit archive",
            },
        }


def build_archive_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job_args["archive"]["file_id"]}'


def assert_valid_archive(archive: BagitArchive) -> None:
    validate_structure(archive)

    # Optional elements (checked first as it may contain checksum of required files)
    validate_any_tagmanifest_file(archive)

    validate_bagit_txt(archive)
    validate_payload(archive)


def validate_structure(archive: BagitArchive) -> None:
    if not archive.build_file_path("bagit.txt") in archive.list_files():
        raise JobException("bagit.txt file not found")

    if not archive.list_manifest_files(archive):
        raise JobException("No manifest file found")

    if not archive.build_file_path("data", is_dir=True) in archive.list_files():
        raise JobException("Payload (data/) directory not found")


def validate_any_tagmanifest_file(archive: BagitArchive) -> None:
    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        tagmanifest_file = archive.build_file_path(f"tagmanifest-{algorithm}.txt")
        if tagmanifest_file not in archive.list_files():
            continue

        for exp_checksum, file_rel_path in parse_manifest_file(
            tagmanifest_file, archive
        ):
            file_path = archive.build_file_path(file_rel_path)
            if file_path not in archive.list_files():
                raise JobException(
                    f"{file_path} referenced by {tagmanifest_file} not found"
                )

            validate_file_checksum(archive, file_path, algorithm, exp_checksum)

        return


def validate_file_checksum(
    archive: BagitArchive, file_path: str, algorithm: str, exp_checksum: str
) -> None:
    with archive.open_file(file_path) as fd:
        data_stream = iter(lambda: fd.read(READ_CHUNK_SIZE), b"")
        checksum = calculate_checksum(data_stream, algorithm)

        if checksum != exp_checksum:
            raise JobException(
                f"{algorithm} checksum verification failed for {file_path}.\n"
                f"Expected: {exp_checksum}, Calculated: {checksum}"
            )


def calculate_checksum(data_stream: Iterator[bytes], algorithm: str) -> str:
    if algorithm == "adler32":
        value = 1
        for data in data_stream:
            value = zlib.adler32(data, value)
        return format(value, "x")
    else:
        hash = hashlib.new(algorithm)
        for data in data_stream:
            hash.update(data)
        return hash.hexdigest()


def validate_bagit_txt(archive: BagitArchive) -> None:
    with archive.open_file(archive.build_file_path("bagit.txt")) as fd:
        lines = fd.readlines()
        if len(lines) != 2:
            raise JobException("Invalid bagit.txt format")

        if not re.match(
            r"^\s*BagIt-Version: [0-9]+.[0-9]+\s*$", lines[0].decode("utf-8")
        ):
            raise JobException(
                "Invalid 'Tag-File-Character-Encoding' definition in 1st line in bagit.txt"
            )
        if not re.match(
            r"^\s*Tag-File-Character-Encoding: \w+", lines[1].decode("utf-8")
        ):
            raise JobException(
                "Invalid 'Tag-File-Character-Encoding' definition in 2nd line in bagit.txt"
            )


def validate_payload(archive: BagitArchive) -> None:
    data_dir = archive.build_file_path("data/")

    payload_files = set()
    for file in archive.list_files():
        if file.startswith(data_dir):
            payload_files.add(file)

    payload_files.update(parse_fetch_file(archive))

    for manifest_file in archive.list_manifest_files():
        referenced_files = set()
        for _, path in parse_manifest_file(manifest_file, archive):
            referenced_files.add(path)

        if payload_files != referenced_files:
            raise JobException(
                f"Files referenced by {manifest_file} do not match with payload files.\n"
                f"  Files in payload but not referenced: {payload_files - referenced_files}\n"
                f"  Files referenced but not in payload: {referenced_files - payload_files}"
            )


def parse_manifest_file(
    manifest_file: str, archive: BagitArchive
) -> List[Tuple[str, str]]:
    checksums = []
    with archive.open_file(manifest_file) as fd:
        for line_num, line in enumerate(fd, start=1):
            try:
                checksum, file_path = line.decode("utf-8").strip().split()
            except Exception:
                raise JobException(
                    f"Failed to parse line number {line_num} in {manifest_file} file"
                )
            else:
                checksums.append((checksum, file_path))

    return checksums


def parse_fetch_file(archive: BagitArchive) -> List[str]:
    fetch_file = archive.build_file_path("fetch.txt")

    if fetch_file not in archive.list_files():
        return []

    files_to_download = []
    for line_num, line in enumerate(archive.open_file(fetch_file), start=1):
        try:
            url, size, path = line.decode("utf-8").strip().split()
            assert size.isnumeric()
        except Exception:
            raise JobException(
                f"Failed to extract url, size and path from line number {line_num} in fetch.txt"
            )
        else:
            if not path.startswith("data/"):
                raise JobException(
                    f"File path not within data/ directory (fetch.txt line {line_num})"
                )
            if not any(url.startswith(schema) for schema in SUPPORTED_URL_SCHEMAS):
                raise JobException(
                    f"URL from line number {line_num} in fetch.txt is not supported"
                )

            files_to_download.append(path)

    return files_to_download
