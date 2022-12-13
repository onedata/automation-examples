"""
A lambda which validates bagit archives and returns correct ones

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
from typing import IO, Final, List, NamedTuple, Optional, Set, Union

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


class ArchiveBytesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="BytesProcessed", unit=None
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
    archive: zipfile



    def get_uncompressed_size(self):
        archive_files = list_archive_files()
        unpacked_archive_size = 0

        for file in archive_files:
            file_info = file.getinfo
            file_size = file_info.file_size
            unpacked_archive_size += file_size

        return unpacked_archive_size


    def find_bagit_dir(self):
        zip_f = ZipFile('macaroon_bag1.zip', 'r')
        root_dirs = []
        for f in zip_f.namelist():

            r_dir = f.split('/')
            r_dir = r_dir[1]
            if r_dir not in root_dirs:
                root_dirs.append(r_dir)
    
                        





class TarArchive:


    def get_uncompressed_size(self):
        archive_files = list_archive_files()
        unpacked_archive_size = 0

        for file in archive_files:
            file_info = file.getinfo
            file_size = file_info.file_size
            unpacked_archive_size += file_size

        return unpacked_archive_size


    def find_bagit_dir(self):
        tar = tarfile.open("macaroon_bag1.tgz")

        # Only root directories:
        root_dirs = []
        for member in tar.getmembers():
            f=member.path
            print(f)

            if "/" in f:
                r_dir = f.split('/')
                r_dir = r_dir[1]
                if r_dir not in root_dirs:
                    root_dirs.append(r_dir)    


class TgzArchive:
    
    
    def get_uncompressed_size(self):
        archive.seek(-4, 2)
        return struct.unpack("I", archive.read(4))[0]


    def find_bagit_dir(self):
        tar = tarfile.open("macaroon_bag1.tgz")

        # Only root directories:
        root_dirs = []
        for member in tar.getmembers():
            f=member.path
            print(f)

            if "/" in f:
                r_dir = f.split('/')
                r_dir = r_dir[1]
                if r_dir not in root_dirs:
                    root_dirs.append(r_dir)      




class AssertBagitException(Exception):
    exception: str


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

        _measurements_queue.put(ArchiveBytesProcessed.build(value=1))
    except AssertBagitException as ex:
        return AtmException(exception=ex)
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job)


def build_job_results(job: Job) -> JobResults:
    return {"result": {"validBagitArchive": job.args["archive"]}}


def assert_valid_bagit_archive(job: Job) -> None:
    archive_path = build_archive_path(job)
    archive_name, archive_type = os.path.splitext(job.args["archive"]["name"])

    if archive_type == ".tar":
        with tarfile.open(archive_path) as archive:
            tarfile_size = tarfile_size(archive.getmembers)
            assert_valid_archive(
                job, archive.getnames, archive.extractfile, archive_type, archive_size
            )
    elif archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            archive_size = get_zip_size(archive.namelist)
            assert_valid_archive(job, archive.namelist, archive.open, archive_type, archive_size)
    elif archive_type == ".tgz" or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            archive_size = get_tgz_size(archive)
            assert_valid_archive(
                job, archive.getnames, archive.extractfile, archive_type,archive_size
            )
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'



def assert_valid_archive(
    job: Job,
    list_archive_files: Callable[[], List[str]],
    open_archive_file: Callable[[str], IO[bytes]],
    archive_type: str,
    archive_size: bytes,
) -> None:


    archive_files = list_archive_files()
    bagit_dir = find_bagit_dir_tar(archive_files)    

    assert_proper_bagit_txt_content(open_archive_file, bagit_dir)

    search_for_fetch_file(archive_files, bagit_dir)

    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        tagmanifest_file = f"{bagit_dir}/tagmanifest-{algorithm}.txt"

        if tagmanifest_file in archive_files:
            checksum_verification(
                job,
                list_archive_files,
                open_archive_file,
                algorithm,
                tagmanifest_file,
                bagit_dir,
                archive_size,
            )
            break


def search_for_fetch_file(archive_files, bagit_dir: str) -> None:
    data_dir = f"{bagit_dir}/data"
    fetch_file = f"{bagit_dir}/fetch.txt"
    if not ((data_dir in archive_files) or (fetch_file in archive_files)):
        raise Exception(
            f"Could not find fetch.txt file or /data directory inside bagit directory: {bagit_dir}."
        )


def checksum_verification(
    job: Job,
    list_archive_files: Callable[[], List[str]],
    open_archive_file: Callable[[str], IO[bytes]],
    algorithm: str,
    tagmanifest_file: str,
    bagit_dir: str,
    archive_size,
) -> None:
    for line in open_archive_file(tagmanifest_file):
        exp_checksum, file_rel_path = line.decode("utf-8").strip().split()
        fd = open_archive_file(f"{bagit_dir}/{file_rel_path}")
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



def calculate_checksum(job: Job, iterator, algorithm: str, archive_size) -> str:
    if algorithm == "adler32":
        value = 1
        read_data = 0
        for data in iterator:
            read_data += READ_CHUNK_SIZE
            value = zlib.adler32(data, value)
            _measurements_queue.put(
                ArchiveBytesProcessed.build(
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
                ArchiveBytesProcessed.build(
                    value=get_bytes_precentage(read_data, archive_size)
                )
            )
            job.heartbeat_callback()
        return hash.hexdigest()


def assert_proper_bagit_txt_content(open_archive_file, bagit_dir) -> None:
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


def get_bytes_precentage(read_data, archive_size):
    return read_data / archive_size
