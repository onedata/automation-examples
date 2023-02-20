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
import traceback
import zipfile
from dataclasses import dataclass
from functools import lru_cache
from threading import Event, Thread
from typing import Dict, Final, Generator, List, NamedTuple, Optional, Union

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


class FileUnpackCtx(NamedTuple):
    job_args: JobArgs

    file_src_path: str
    file_data_dir_rel_path: str

    dst_dir: str
    file_dst_path: str


@dataclass
class FileUnpackProgress:
    target_size: int
    current_size: int


class BagitArchiveFileInfo(abc.ABC):
    @abc.abstractmethod
    def get_path(self) -> str:
        pass

    @abc.abstractmethod
    def is_dir(self) -> bool:
        pass

    @abc.abstractmethod
    def get_size(self) -> int:
        pass

    @abc.abstractmethod
    def substitute_path(self, new_path: str) -> None:
        pass


class ZipBagitArchiveFileInfo(BagitArchiveFileInfo):
    def __init__(self, info: zipfile.ZipInfo) -> None:
        self.info = info

    def get_path(self) -> str:
        return self.info.filename

    def is_dir(self) -> bool:
        return self.info.is_dir()

    def get_size(self) -> int:
        return self.info.file_size

    def substitute_path(self, new_path: str) -> None:
        self.info.filename = new_path


class TarBagitArchiveFileInfo(BagitArchiveFileInfo):
    def __init__(self, info: tarfile.TarInfo) -> None:
        self.info = info

    def get_path(self) -> str:
        return self.info.name

    def is_dir(self) -> bool:
        return self.info.isdir()

    def get_size(self) -> int:
        return self.info.size

    def substitute_path(self, new_path: str) -> None:
        self.info.name = new_path


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
    def list_members(self) -> List[BagitArchiveFileInfo]:
        pass

    @abc.abstractmethod
    def get_file_info(self, path: str) -> BagitArchiveFileInfo:
        pass

    @abc.abstractmethod
    def unpack_file(self, file_info: BagitArchiveFileInfo, dst_dir: str) -> None:
        pass


class ZipBagitArchive(BagitArchive):
    def __init__(self, archive: zipfile.ZipFile) -> None:
        self.archive = archive

    def build_file_path(self, file_rel_path: str, *, is_dir=False) -> str:
        return f"{self.get_bagit_dir_name()}/{file_rel_path}{'/' if is_dir else ''}"

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.namelist()

    def list_members(self) -> List[ZipBagitArchiveFileInfo]:
        return list(map(ZipBagitArchiveFileInfo, self.archive.infolist()))

    def get_file_info(self, path: str) -> ZipBagitArchiveFileInfo:
        return ZipBagitArchiveFileInfo(self.archive.getinfo(path))

    def unpack_file(self, file_info: ZipBagitArchiveFileInfo, dst_dir: str) -> None:
        self.archive.extract(file_info.info, dst_dir)


class TarBagitArchive(BagitArchive):
    def __init__(self, archive: tarfile.TarFile):
        self.archive = archive

    def build_file_path(self, file_rel_path: str, **kwargs) -> str:
        return f"{self.get_bagit_dir_name()}/{file_rel_path}"

    @lru_cache
    def list_files(self) -> List[str]:
        return self.archive.getnames()

    def list_members(self) -> List[TarBagitArchiveFileInfo]:
        return list(map(TarBagitArchiveFileInfo, self.archive.getmembers()))

    def get_file_info(self, path: str) -> TarBagitArchiveFileInfo:
        return TarBagitArchiveFileInfo(self.archive.getmember(path))

    def unpack_file(self, file_info: TarBagitArchiveFileInfo, dst_dir: str) -> None:
        self.archive.extract(file_info.info, dst_dir)


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


_all_archives_unpacked: Event = Event()
_files_to_monitor_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    unpacking_monitor = Thread(
        target=monitor_files_unpacking, daemon=True, args=[heartbeat_callback]
    )
    unpacking_monitor.start()

    job_results = [run_job(job_args) for job_args in job_batch_request["argsBatch"]]

    _all_archives_unpacked.set()
    unpacking_monitor.join()

    return {"resultsBatch": job_results}


def run_job(job_args: JobArgs) -> Union[AtmException, JobResults]:
    try:
        unpacked_files = unpack_bagit_archive(job_args)
    except JobException as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            "unpackedFiles": unpacked_files,
            "statusLog": {
                "archive": job_args["archive"]["name"],
                "status": f"Successfully unpacked {len(unpacked_files)} files.",
            },
        }


def unpack_bagit_archive(job_args: JobArgs) -> List[str]:
    dst_dir = build_dst_dir_path(job_args)

    files_to_unpack = []
    with open_archive(job_args) as archive:
        data_dir = archive.build_file_path("data", is_dir=True)

        for file_info in archive.list_members():
            file_src_path = file_info.get_path()

            if file_src_path.startswith(data_dir) and not file_info.is_dir():
                file_data_dir_rel_path = file_src_path[len(data_dir):].lstrip("/")
                file_dst_path = f"{dst_dir}/{file_data_dir_rel_path}"

                files_to_unpack.append(
                    FileUnpackCtx(
                        job_args,
                        file_src_path,
                        file_data_dir_rel_path,
                        dst_dir,
                        file_dst_path,
                    )
                )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        unpacked_files = list(executor.map(unpack_file, files_to_unpack))

    return unpacked_files


def unpack_file(file_unpack_ctx: FileUnpackCtx) -> str:
    with open_archive(file_unpack_ctx.job_args) as archive:
        file_info = archive.get_file_info(file_unpack_ctx.file_src_path)

        file_size = file_info.get_size()
        _files_to_monitor_queue.put((file_unpack_ctx.file_dst_path, file_size))

        # Tamper with file path so that when unpacking only parent directories 
        # up to data directory will be created in destination directory 
        # (normally all directories on path are created)
        file_info.substitute_path(file_unpack_ctx.file_data_dir_rel_path)
        archive.unpack_file(file_info, file_unpack_ctx.dst_dir)

        return file_unpack_ctx.file_dst_path


def build_archive_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job_args["archive"]["file_id"]}'


def build_dst_dir_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job_args["destinationDir"]["file_id"]}'


def monitor_files_unpacking(heartbeat_callback: AtmHeartbeatCallback) -> None:
    monitored_files: Dict[str, FileUnpackProgress] = {}
    any_unpacking_ongoing = True

    while any_unpacking_ongoing or monitored_files:
        any_unpacking_ongoing = not _all_archives_unpacked.wait(timeout=1)

        while not _files_to_monitor_queue.empty():
            file_path, target_size = _files_to_monitor_queue.get()
            monitored_files[file_path] = FileUnpackProgress(target_size, 0)

        bytes_unpacked = 0
        files_unpacked = 0
        for file_path, progress in list(monitored_files.items()):
            file_size = get_file_size(file_path)

            bytes_unpacked += file_size - progress.current_size

            if file_size >= progress.target_size:
                files_unpacked += 1
                monitored_files.pop(file_path)
            else:
                progress.current_size = file_size

        measurements = []
        if bytes_unpacked:
            measurements.append(BytesUnpacked.build(value=bytes_unpacked))
        if files_unpacked:
            measurements.append(FilesUnpacked.build(value=files_unpacked))

        if measurements:
            STATS_STREAMER.stream_items(measurements)
            heartbeat_callback()


def get_file_size(file_path: str) -> int:
    try:
        return os.stat(file_path).st_size
    except:
        return 0
