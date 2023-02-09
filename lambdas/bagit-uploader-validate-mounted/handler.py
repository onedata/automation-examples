"""
A lambda which validates bagit archives.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import abc
import concurrent.futures
import contextlib
import hashlib
import os
import os.path
import queue
import re
import tarfile
import traceback
import zipfile
import zlib
from functools import lru_cache
from threading import Event, Thread
from typing import IO, Final, Generator, Iterator, List, Optional, Set

from onedata_lambda_utils.stats import AtmTimeSeriesMeasurementBuilder
from onedata_lambda_utils.streaming import AtmResultStreamer
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
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


class BytesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"
):
    pass


class JobArgs(TypedDict):
    archive: AtmFile


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    pass


class BagitArchive(abc.ABC):
    def get_bagit_dir_name(self):
        if getattr(self, "_bagit_dir_name", None) is None:
            for file in self.list_files():
                path_tokens = file.split("/")
                if len(path_tokens) == 2 and path_tokens[1] == "bagit.txt":
                    self._bagit_dir_name = path_tokens[0]
                    break
            else:
                raise JobException("Bagit directory not found.")

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
        return self.archive.extractfile(path)


@contextlib.contextmanager
def open_archive(
    archive_path: str, archive_type: str
) -> Generator[BagitArchive, None, None]:
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


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[Optional[AtmException]]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        job_results = list(executor.map(run_job, job_batch_request["argsBatch"]))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": job_results}


def run_job(job_args: JobArgs) -> Optional[AtmException]:
    try:
        if job_args["archive"]["type"] != "REG":
            return AtmException(exception=("Not an archive file."))

        archive_path = build_archive_path(job_args)
        _, archive_type = os.path.splitext(job_args["archive"]["name"])

        with open_archive(archive_path, archive_type) as archive:
            assert_valid_archive(archive)

    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return None


def build_archive_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job_args["archive"]["file_id"]}'


def assert_valid_archive(archive: BagitArchive) -> None:
    # Report 0 bytesProcessed to signal that thread is alive and running
    # so that heartbeat can be sent (needed in case when below functions
    # do not stream any measurements due to e.g. empty data directory)
    _measurements_queue.put(BytesProcessed.build(value=0))

    # Optional elements (checked first as it may contain checksum of required files)
    validate_checksum_file(archive, "tagmanifest", is_optional=True)

    # Required elements
    validate_bagit_txt_content(archive)
    validate_data_dir_presence(archive)
    validate_checksum_file(archive, "manifest", is_optional=False)


def validate_bagit_txt_content(archive: BagitArchive) -> None:
    with archive.open_file(archive.build_file_path("bagit.txt")) as fd:
        line1, line2 = fd.readlines()
        if not re.match(r"^\s*BagIt-Version: [0-9]+.[0-9]+\s*$", line1.decode("utf-8")):
            raise JobException(
                "Invalid 'Tag-File-Character-Encoding' definition in 1st line in bagit.txt."
            )
        if not re.match(r"^\s*Tag-File-Character-Encoding: \w+", line2.decode("utf-8")):
            raise JobException(
                "Invalid 'Tag-File-Character-Encoding' definition in 2nd line in bagit.txt."
            )


def validate_data_dir_presence(archive: BagitArchive) -> None:
    if not archive.build_file_path("data", is_dir=True) in archive.list_files():
        raise JobException(
            "Could not find fetch.txt file or /data directory inside bagit directory."
        )


def validate_checksum_file(
    archive: BagitArchive, name_prefix: str, *, is_optional: bool
) -> None:
    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        checksum_file = archive.build_file_path(f"{name_prefix}-{algorithm}.txt")
        if checksum_file not in archive.list_files():
            continue

        with archive.open_file(checksum_file) as fd:
            for line in fd:
                exp_checksum, file_rel_path = line.decode("utf-8").strip().split()
                file_path = archive.build_file_path(file_rel_path)
                validate_file_checksum(archive, file_path, algorithm, exp_checksum)

        return

    else:
        if not is_optional:
            raise JobException(f"{name_prefix} file not found.")


def validate_file_checksum(
    archive: BagitArchive, file_path: str, algorithm: str, exp_checksum: str
) -> None:
    with archive.open_file(file_path) as fd:
        data_stream = iter(lambda: fd.read(READ_CHUNK_SIZE), b"")
        checksum = calculate_checksum(data_stream, algorithm)

        if checksum != exp_checksum:
            raise JobException(
                f"{algorithm} checksum verification failed for file {file_path}.\n"
                f"Expected: {exp_checksum}, Calculated: {checksum}"
            )


def calculate_checksum(data_stream: Iterator[bytes], algorithm: str) -> str:
    if algorithm == "adler32":
        value = 1
        for data in data_stream:
            value = zlib.adler32(data, value)
            _measurements_queue.put(BytesProcessed.build(value=len(data)))
        return format(value, "x")
    else:
        hash = hashlib.new(algorithm)
        for data in data_stream:
            hash.update(data)
            _measurements_queue.put(BytesProcessed.build(value=len(data)))
        return hash.hexdigest()


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
