"""
A lambda which unpack files from archive /data directory 
and puts them under destination directory.

"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import os
import queue
import os.path
import tarfile
from threading import Event, Thread
import time
import traceback
import zipfile
from pathlib import Path
from typing import Callable, Final, List, NamedTuple, Optional, Union

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


class ZipArchive:
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive

    def find_bagit_directory(self) -> str:
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

    def unpack_archive_file(self, file_archive_info, destination_diretory) -> str:
        return self.archive.extract(file_archive_info, destination_diretory)

    def get_archive_member(self, name) -> Callable[[], tarfile.TarInfo]:
        return self.archive.getinfo(name)


class TarArchive:
    def __init__(self, archive: tarfile.TarFile):
        self.archive = archive

    def find_bagit_directory(self) -> str:
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

    def unpack_archive_file(self, file_archive_info, destination_diretory) -> None:
        return self.archive.extract(file_archive_info, destination_diretory)

    def get_archive_member(self, name) -> Callable[[], tarfile.TarInfo]:
        return self.archive.getmember(name)


class TgzArchive:
    def __init__(self, archive: tarfile.TarFile) -> None:
        self.archive = archive

    def find_bagit_directory(self) -> str:
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

    def unpack_archive_file(self, file_archive_info, destination_diretory) -> None:
        return self.archive.extract(file_archive_info, destination_diretory)

    def get_archive_member(self, name) -> Callable[[], tarfile.TarInfo]:
        return self.archive.getmember(name)


class UnpackFileData(NamedTuple):
    job: Job
    archive: Union[ZipArchive, TarArchive, TgzArchive]
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

    if archive_type == ".tar":
        with tarfile.TarFile(archive_path) as archive:
            tar_archive = TarArchive(archive)
            return unpack_data_directory(job, tar_archive)
    elif archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            zip_archive = ZipArchive(archive)
            return unpack_data_directory(job, zip_archive)
    elif archive_type == ".tgz" or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            tgz_archive = TgzArchive(archive)
            return unpack_data_directory(job, tgz_archive)
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def unpack_data_directory(
    job: Job, archive: Union[ZipArchive, TarArchive, TgzArchive]
) -> List[str]:

    archive_files = archive.list_archive_files()
    bagit_directory = archive.find_bagit_directory()
    data_directory = f"{bagit_directory}/data/"

    unpacked_files=[]

    # extracting large files may last for a long time, therefore monitor thread with heartbeats is needed
    # unpacked_monitor = Thread(
    #     target=monitor_unpack,
    #     args=(job),
    #     daemon=True,
    # )
    # unpacked_monitor.start()

    # unpacked_archive_files = [
        # UnpackFileData(
        #     job,
        #     archive,
        #     data_directory,
        #     file_path,
        # )
    #     for file_path in archive_files
    #     if file_path.startswith(data_directory) and (not file_path.endswith("/"))
    # ]

    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     unpacked_files = list(executor.map(unpack_file, unpacked_archive_files))

    # _all_archives_unpacked.set()
    # unpacked_monitor.join()

    for file_path in archive_files:
        is_directory = file_path.endswith("/")
        if file_path.startswith(data_directory) and (not is_directory):
            try:
                unpacked_file_data = UnpackFileData(job,archive, data_directory,file_path,)
                destination_path = unpack_file (unpacked_file_data)
                unpacked_files.append(destination_path)
            except Exception as ex:
                raise Exception(f"Unpacking file {file_path} failed due to: {str(ex)}")
    

    return unpacked_files


def unpack_file(file: UnpackFileData) -> str:

    destination_diretory = build_destination_directory_path(file.job)
    subpath = file.file_path[len(file.data_directory) :]
    file_archive_info = file.archive.get_archive_member(file.file_path)

    if hasattr(file_archive_info, "name"):
        file_archive_info.name = subpath
        file_size = file_archive_info.size
    else:
        file_archive_info.filename = subpath
        file_size = file_archive_info.file_size

    destination_path = f"{destination_diretory}/{subpath}"
    _files_queue.put(FileInfo(file.file_path, file_size))

    file.archive.unpack_archive_file(file_archive_info, destination_diretory)

    return destination_path


def build_archive_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["archive"]["file_id"]}'


def build_destination_directory_path(job: Job) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job.args["destinationDir"]["file_id"]}'


def monitor_unpack_file(job: Job) -> None:
    # any_archive_unpacking = True

    # while any_archive_unpacking:
    #     any_archive_unpacking = not _all_archives_unpacked.wait(timeout=1)
    #     time.sleep(0.5)

    #     measurements = []
    #     while not _files_queue.empty():
    #         file = _files_queue.get()

    #     if measurements:
    #         STATS_STREAMER.stream_items(measurements)

    #     size = 0
    #     while size < file.file_size:
    #         current_size = Path(file.file_path).stat().st_size
    #         if current_size > size:
    #             job.heartbeat()
    #             measurements.append(BytesUnpacked.build(value=(size - current_size)))
    #             size = current_size

    #     measurements.append(FilesUnpacked.build(value=1)) 
    pass
