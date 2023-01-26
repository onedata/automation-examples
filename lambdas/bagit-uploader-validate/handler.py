"""
A lambda which validates bagit archives and returns result in form of archive object
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import hashlib
import os
import os.path
import queue
import re
import struct
import tarfile
import traceback
import zipfile
import zlib
from collections.abc import Callable
from threading import Event, Thread
from typing import IO, Final, Iterator, List, NamedTuple, Optional, Set, Union

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

AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"adler32"}, hashlib.algorithms_available
)
READ_CHUNK_SIZE: Final[int] = 10 * 1024**2


##===================================================================
## Lambda interface
##===================================================================


STATS_STREAMER: Final[AtmResultStreamer[AtmTimeSeriesMeasurement]] = AtmResultStreamer(
    result_name="stats", synchronized=False
)


class ArchivePercentProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="percentProcessed", unit=None
):
    pass


class JobArgs(TypedDict):
    archive: AtmFile


class FileBagitReport(TypedDict):
    validBagitArchive: AtmFile


class JobResults(TypedDict):
    result: FileBagitReport


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    heartbeat_callback: AtmHeartbeatCallback
    args: JobArgs


class ZipArchive:
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive

    def get_uncompressed_size(self) -> int:
        archive_files = self.archive.namelist()
        unpacked_archive_size = 0

        for file in archive_files:
            file_info = file.getinfo
            file_size = file_info.file_size
            unpacked_archive_size += file_size

        return unpacked_archive_size

    def find_bagit_dir(self) -> str:
        root_files = []

        for file in self.archive.namelist():
            path = file.split("/")
            root_directory = path[0]
            root_file = path[1]
            if root_file not in root_files:
                root_files.append(root_file)

        if "bagit.txt" in root_files:
            return root_directory

    def list_archive_files(self) -> List[str]:
        return self.archive.namelist()

    def open_archive_file(self, path):
        return self.archive.open(path)


class TarArchive:
    def __init__(self, archive: tarfile.TarFile):
        self.archive = archive

    def get_uncompressed_size(self) -> int:
        archive_files = self.archive.getmembers()
        unpacked_archive_size = 0

        for file in archive_files:
            file_size = file.size
            unpacked_archive_size += file_size

        return unpacked_archive_size

    def find_bagit_dir(self) -> str:
        root_files = []

        for member in self.archive.getmembers():
            file = member.path

            if "/" in file:
                path = file.split("/")
                root_directory = path[0]
                root_file = path[1]
                if root_file not in root_files:
                    root_files.append(root_file)

        if "bagit.txt" in root_files:
            return root_directory

    def list_archive_files(self) -> List[str]:
        return self.archive.getnames()

    def open_archive_file(self, path):
        return self.archive.extractfile(path)


class TgzArchive:
    def __init__(self, archive: tarfile.TarFile) -> None:
        self.archive = archive

    def get_uncompressed_size(self) -> tuple:
        self.archive.seek(-4, 2)
        return struct.unpack("I", self.archive.read(4))[0]

    def find_bagit_dir(self) -> str:
        root_files = []

        for member in self.archive.getmembers():
            file = member.path

            if "/" in file:
                path = file.split("/")
                root_directory = path[0]
                root_file = path[1]
                if root_file not in root_files:
                    root_files.append(root_file)

        if "bagit.txt" in root_files:
            return root_directory

    def list_archive_files(self) -> List[str]:
        return self.archive.getnames()

    def open_archive_file(self, path):
        return self.archive.extractfile(path)


class AssertBagitException(Exception):
    pass


_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    job_results = []
    for job_args in job_batch_request["argsBatch"]:
        job = Job(args=job_args, heartbeat_callback=heartbeat_callback)
        job_results.append(run_job(job))

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults]:
    try:
        if job.args["archive"]["type"] != "REG":
            return AtmException(exception=(f"Archive file is unsupported."))

        assert_valid_bagit_archive(job)

        _measurements_queue.put(ArchivePercentProcessed.build(value=1))
    except AssertBagitException as ex:
        return AtmException(exception=ex)
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job)


def build_job_results(job: Job) -> JobResults:
    return {"validBagitArchive": job.args["archive"]}


def assert_valid_bagit_archive(job: Job) -> None:
    archive_path = build_archive_path(job)
    archive_name, archive_type = os.path.splitext(job.args["archive"]["name"])

    if archive_type == ".tar":
        with tarfile.open(archive_path) as archive:
            tar_archive = TarArchive(archive)
            assert_valid_archive(job, tar_archive)
    elif archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            zip_archive = ZipArchive(archive)
            assert_valid_archive(job, zip_archive)
    elif archive_type == ".tgz" or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            tgz_archive = TgzArchive(archive)
            assert_valid_archive(job, tgz_archive)
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'


def assert_valid_archive(
    job: Job, archive: Union[ZipArchive, TarArchive, TgzArchive]
) -> None:

    bagit_dir = archive.find_bagit_dir()
    archive_size = archive.get_uncompressed_size()
    archive_files = archive.list_archive_files()

    assert_proper_bagit_txt_content(archive.open_archive_file, bagit_dir)

    search_for_fetch_file(archive_files, bagit_dir)

    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        tagmanifest_file = f"{bagit_dir}/tagmanifest-{algorithm}.txt"

        if tagmanifest_file in archive_files:
            checksum_verification(
                job,
                archive,
                algorithm,
                tagmanifest_file,
                bagit_dir,
                archive_size,
            )
            break


def search_for_fetch_file(archive_files: list, bagit_dir: str) -> None:
    data_dir = f"{bagit_dir}/data"
    fetch_file = f"{bagit_dir}/fetch.txt"
    if not ((data_dir in archive_files) or (fetch_file in archive_files)):
        raise Exception(
            f"Could not find fetch.txt file or /data directory inside bagit directory: {bagit_dir}."
        )


def checksum_verification(
    job: Job,
    archive: Union[ZipArchive, TarArchive, TgzArchive],
    algorithm: str,
    tagmanifest_file: str,
    bagit_dir: str,
    archive_size: int,
) -> None:
    for line in archive.open_archive_file(tagmanifest_file):
        exp_checksum, file_rel_path = line.decode("utf-8").strip().split()
        fd = archive.open_archive_file(f"{bagit_dir}/{file_rel_path}")
        iterator = iter(lambda: fd.read(READ_CHUNK_SIZE), b"")
        calculated_checksum = calculate_checksum(job, iterator, algorithm, archive_size)
        if not exp_checksum == calculated_checksum:
            raise Exception(
                f"{algorithm} checksum verification failed for file {file_rel_path}. \n"
                f"Expected: {exp_checksum}, Calculated: {calculated_checksum}"
            )


def find_bagit_dir_zip(archive_files: list) -> Optional[str]:

    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == "bagit.txt":
            return dir_path

    raise Exception("Could not find bagit.txt file in bagit directory.")


def calculate_checksum(
    job: Job, iterator: Iterator[bytes], algorithm: str, archive_size: int
) -> str:
    if algorithm == "adler32":
        value = 1
        read_data = 0
        for data in iterator:
            read_data += READ_CHUNK_SIZE
            value = zlib.adler32(data, value)
            _measurements_queue.put(
                ArchivePercentProcessed.build(
                    value=get_bytes_precentage(read_data, archive_size)
                )
            )
            job.heartbeat_callback()
        return format(value, "x")
    else:
        read_data = 0
        hash = getattr(hashlib, algorithm)()
        for data in iterator:
            hash.update(data)
            read_data += READ_CHUNK_SIZE
            _measurements_queue.put(
                ArchivePercentProcessed.build(
                    value=get_bytes_precentage(read_data, archive_size)
                )
            )
            job.heartbeat_callback()
        return hash.hexdigest()


def assert_proper_bagit_txt_content(
    open_archive_file: Callable[[str], IO[bytes]], bagit_dir: str
) -> None:
    with open_archive_file(f"{bagit_dir}/bagit.txt") as fd:
        line1, line2 = fd.readlines()
        if not re.match(r"^\s*BagIt-Version: [0-9]+.[0-9]+\s*$", line1.decode("utf-8")):
            raise Exception(
                f"Invalid 'Tag-File-Character-Encoding' definition in line /1 in bagit.txt. Incorrect content: {line1}"
            )
        if not re.match(r"^\s*Tag-File-Character-Encoding: \w+", line2.decode("utf-8")):
            raise Exception(
                f"Invalid 'Tag-File-Character-Encoding' definition in line 2 in bagit.txt. Incorrect content: {line2}"
            )


def get_bytes_precentage(read_data: int, archive_size: int) -> int:
    return read_data / archive_size
