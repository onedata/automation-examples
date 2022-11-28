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


class FilesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="filesProcessed", unit=None
):
    pass


class BytesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"
):
    pass


class JobArgs(TypedDict):
    archive: AtmFile


class FileBagitReport(TypedDict):
    validBagitArchive: str


class JobResults(TypedDict):
    result: FileBagitReport


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    jobs = [
        Job(args=job_args, ctx=job_batch_request["ctx"])
        for job_args in job_batch_request["argsBatch"]
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        job_results = list(executor.map(run_job, jobs))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults]:
    try:
        if job.args["archive"]["type"] != "REG":
            return build_job_results(job)

        assert_valid_bagit_archive(job)

        _measurements_queue.put(FilesProcessed.build(value=1))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    return build_job_results(job)


def build_job_results(job: Job) -> JobResults:
    return {"result": {"validBagitArchive": job["archive"]}}


def assert_valid_bagit_archive(job: Job) -> None:
    archive_path = build_archive_path(job)
    archive_name, archive_type = os.path.splitext(job.args["archive"]["name"])

    if archive_type == ".tar":
        with tarfile.open(archive_path) as archive:
            assert_valid_archive(archive.getnames, archive.extractfile)
    elif archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            assert_valid_archive(archive.namelist, archive.open)
    elif archive_type == ".tgz" or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            assert_valid_archive(archive.getnames, archive.extractfile)
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'


def assert_valid_archive(
    list_archive_files: Callable[[], List[str]],
    open_archive_file: Callable[[str], IO[bytes]],
) -> None:
    archive_files = list_archive_files()
    bagit_dir = find_bagit_dir(archive_files)

    assert_proper_bagit_txt_content(open_archive_file, bagit_dir)

    search_for_fetch_file(archive_files, bagit_dir)

    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        tagmanifest_file = f"{bagit_dir}/tagmanifest-{algorithm}.txt"

        if tagmanifest_file in archive_files:
            checksum_verification(
                list_archive_files, open_archive_file, tagmanifest_file, bagit_dir
            )


def search_for_fetch_file(archive_files, bagit_dir: str):
    data_dir = f"{bagit_dir}/data"
    fetch_file = f"{bagit_dir}/fetch.txt"
    if not (
        (data_dir in archive_files)
        or (fetch_file in archive_files)
    ):
        raise Exception(
            f"Could not find fetch.txt file or /data directory inside bagit directory: {bagit_dir}."
        )


def checksum_verification(
    list_archive_files: Callable[[], List[str]],
    open_archive_file: Callable[[str], IO[bytes]],
    algorithm: str,
    tagmanifest_file: str,
    bagit_dir: str,
) -> None:
    for line in open_archive_file(tagmanifest_file):
        exp_checksum, file_rel_path = line.decode("utf-8").strip().split()
        fd = open_archive_file(f"{bagit_dir}/{file_rel_path}")
        calculated_checksum = calculate_checksum(fd, algorithm)
        if not exp_checksum == calculated_checksum:
            raise Exception(
                f"{algorithm} checksum verification failed for file {file_rel_path}. \n"
                f"Expected: {exp_checksum}, Calculated: {calculated_checksum}"
            )


def find_bagit_dir(archive_files: list) -> Optional[str]:
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == "bagit.txt":
            return dir_path

    raise Exception("Could not find bagit.txt file in bagit directory.")


def calculate_checksum(fd, algorithm: str) -> str:
    if algorithm == "adler32":
        value = 1
        for data in iter(lambda: fd.read(READ_CHUNK_SIZE), b""):
            value = zlib.adler32(data, value)
            _measurements_queue.put(BytesProcessed.build(value=len(data)))
        return format(value, "x")
    else:
        hash = getattr(hashlib, algorithm)()
        for data in iter(lambda: fd.read(READ_CHUNK_SIZE), b""):
            hash.update(data)
            _measurements_queue.put(BytesProcessed.build(value=len(data)))
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


def monitor_jobs(heartbeat_callback: AtmHeartbeatCallback) -> None:
    any_job_ongoing = True
    while any_job_ongoing:
        any_job_ongoing = not _all_jobs_processed.wait(timeout=1)

        measurements = []
        while not _measurements_queue.empty():
            measurements.append(_measurements_queue.get())

        if measurements:
            STATS_STREAMER.stream_items(measurements)
            heartbeat_callback()
