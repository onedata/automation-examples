"""
A lambda which unpack files from archive /data directory 
and puts them under destination directory.

"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import abc
import concurrent.futures
import contextlib
import os
import os.path
import queue
import tarfile
import time
import traceback
import zipfile
from functools import lru_cache
from pathlib import Path
from threading import Event, Thread
from typing import Callable, Final, Generator, List, NamedTuple, Optional, Union

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


##===================================================================
## Lambda interface
##===================================================================


STATS_STREAMER: Final[AtmResultStreamer[AtmTimeSeriesMeasurement]] = AtmResultStreamer(
    result_name="stats", synchronized=False
)


class FilesUnpacked(
    AtmTimeSeriesMeasurementBuilder, ts_name="filesUnpacked", unit=None
):
    pass


class BytesUnpacked(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesUnpacked", unit="Bytes"
):
    pass


class JobArgs(TypedDict):
    archive: AtmFile
    destinationDir: AtmFile


class LogsReport(TypedDict):
    archive: str
    status: Optional[str]


class JobResults(TypedDict):
    unpackedFiles: List[str]
    statusLog: LogsReport


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    exception: str


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
                raise JobException("Bagit directory not found.")

        return self._bagit_dir_name

    @abc.abstractmethod
    def build_file_path(self, file_rel_path: str, *, is_dir: bool = False) -> str:
        pass

    @abc.abstractmethod
    def list_files(self) -> List[str]:
        pass

    @abc.abstractmethod
    def unpack_file(
        self,
        file_archive_info: Union[zipfile.ZipInfo, tarfile.TarInfo],
        destination_diretory: str,
    ) -> str:
        pass

    @abc.abstractmethod
    def get_archive_member(self, name: str) -> Callable[[], zipfile.ZipInfo]:
        pass

    @abc.abstractmethod
    def close_archive(self) -> None:
        pass


class ZipBagitArchive(BagitArchive):
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive

    def build_file_path(self, file_rel_path: str, *, is_dir=False) -> str:
        return f"{self.get_bagit_dir_name()}/{file_rel_path}{'/' if is_dir else ''}"

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.namelist()

    def unpack_file(
        self,
        file_archive_info: Union[zipfile.ZipInfo, tarfile.TarInfo],
        destination_diretory: str,
    ) -> str:
        return self.archive.extract(file_archive_info, destination_diretory)

    def get_archive_member(self, name: str) -> Callable[[], zipfile.ZipInfo]:
        return self.archive.getinfo(name)

    def close_archive(self) -> None:
        self.archive.close()


class TarBagitArchive(BagitArchive):
    def __init__(self, archive: tarfile.TarFile):
        self.archive = archive

    def build_file_path(self, file_rel_path: str, **kwargs) -> str:
        return f"{self.get_bagit_dir_name()}/{file_rel_path}"

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.getnames()

    def unpack_file(
        self,
        file_archive_info: Union[zipfile.ZipInfo, tarfile.TarInfo],
        destination_diretory: str,
    ) -> None:
        return self.archive.extract(file_archive_info, destination_diretory)

    def get_archive_member(self, name: str) -> Callable[[], tarfile.TarInfo]:
        return self.archive.getmember(name)

    def close_archive(self) -> None:
        self.archive.close()


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


class UnpackFileData(NamedTuple):
    job: Job
    data_directory: str
    file_path: str


class FileInfo(NamedTuple):
    file_path: str
    file_size: int


_all_archives_unpacked: Event = Event()
_files_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    job_results = []
    for job_args in job_batch_request["argsBatch"]:
        job = Job(args=job_args, heartbeat_callback=heartbeat_callback)
        job_results.append(run_job(job))

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults, LogsReport]:
    try:
        unpacked_files = unpack_bagit_archive(job)
    except JobException as ex:
        return AtmException(exception=build_error_logs(job, ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job, unpacked_files)


def build_error_logs(job: Job, ex: str) -> LogsReport:
    return {
        "file": job.args["archive"]["name"],
        "status": f"Failed to unpack files due to: {str(ex)}",
    }


def build_job_results(job: Job, unpacked_files: List[str]) -> JobResults:
    return {
        "unpackedFiles": unpacked_files,
        "statusLog": {
            "archive": job.args["archive"]["name"],
            "status": f"Successfully unpacked {len(unpacked_files)} files.",
        },
    }


def unpack_bagit_archive(job: Job) -> List[str]:
    archive_filename = job.args["archive"]["name"]
    archive_path = build_archive_path(job)
    archive_name, archive_type = os.path.splitext(archive_filename)

    with open_archive(archive_path, archive_type) as archive:
        archive_files = archive.list_files()
        data_directory = archive.build_file_path("data", is_dir=True)
        archive.close_archive()

    unpacked_files = []

    unpacked_monitor = Thread(
        target=monitor_unpack_file,
        args=(job),
        daemon=True,
    )
    unpacked_monitor.start()

    unpacked_archive_files = [
        UnpackFileData(
            job,
            data_directory,
            file_path,
        )
        for file_path in archive_files
        if file_path.startswith(data_directory) and (not file_path.endswith("/"))
    ]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        unpacked_files = list(
            executor.map(unpack_data_directory_file, unpacked_archive_files)
        )

    _all_archives_unpacked.set()
    unpacked_monitor.join()

    return unpacked_files


def unpack_data_directory_file(unpack_info: UnpackFileData) -> str:

    archive_filename = unpack_info.job.args["archive"]["name"]
    archive_path = build_archive_path(unpack_info.job)
    archive_name, archive_type = os.path.splitext(archive_filename)

    with open_archive(archive_path, archive_type) as archive:
        subpath = unpack_info.file_path[len(unpack_info.data_directory) :]
        file_archive_info = archive.get_archive_member(unpack_info.file_path)

        if hasattr(file_archive_info, "name"):
            file_archive_info.name = subpath
            file_size = file_archive_info.size
        else:
            file_archive_info.filename = subpath
            file_size = file_archive_info.file_size

        destination_diretory = build_destination_directory_path(unpack_info.job)
        destination_path = f"{destination_diretory}/{subpath}"
        _files_queue.put(FileInfo(unpack_info.file_path, file_size))

        archive.unpack_file(file_archive_info, destination_diretory)

    return destination_path


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'


def build_destination_directory_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["destinationDir"]["file_id"]}'


def monitor_unpack_file(job: Job) -> None:
    any_archive_unpacking = True

    while any_archive_unpacking:
        any_archive_unpacking = not _all_archives_unpacked.wait(timeout=1)
        time.sleep(0.5)

        measurements = []
        while not _files_queue.empty():
            file = _files_queue.get()

        if measurements:
            STATS_STREAMER.stream_items(measurements)

        size = 0
        while size < file.file_size:
            current_size = Path(file.file_path).stat().st_size
            if current_size > size:
                job.heartbeat()
                measurements.append(BytesUnpacked.build(value=(size - current_size)))
                size = current_size

        measurements.append(FilesUnpacked.build(value=1))
