"""
A lambda which unpack files from archive /data directory 
and puts them under destination directory.

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
from pathlib import Path
from threading import Event, Thread
from typing import IO, Final, Iterator, NamedTuple, Optional, Set, Union

import requests
from openfaas_lambda_utils.stats import AtmTimeSeriesMeasurementBuilder
from openfaas_lambda_utils.streaming import AtmResultStreamer
from openfaas_lambda_utils.types import (
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


class FilesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="filesProcessed", unit=None
):
    pass


class BytesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"
):
    pass


class JobArgs(TypedDict):
    file: AtmFile
    algorithm: str
    metadataKey: str


class FileChecksumReport(TypedDict):
    file_id: str
    algorithm: str
    checksum: Optional[str]


class JobResults(TypedDict):
    result: FileChecksumReport


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(request: dict, heartbeat_callback) -> str:

    return {
        "resultsBatch": [
            process_item(item, request["ctx"], heartbeat_callback)
            for item in request["argsBatch"]
        ]
    }


def process_item(args: dict, ctx: dict, heartbeat_callback):

    try:
        uploaded_files = unpack_data_dir(args, ctx, heartbeat_callback)
        return {
            "uploadedFiles": uploaded_files,
            "logs": [
                {
                    "severity": "info",
                    "file": args["archive"]["name"],
                    "status": f"Successfully uploaded {len(uploaded_files)} files.",
                }
            ],
        }
    except Exception as ex:
        return {
            "exception": [
                {
                    "severity": "error",
                    "file": args["archive"]["name"],
                    "status": f"Failed to unpack files due to: {str(ex)}",
                }
            ]
        }


def unpack_data_dir(args: dict, ctx: dict, heartbeat_callback) -> list:
    archive_filename = args["archive"]["name"]
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == ".tar":
        with tarfile.TarFile(archive_path) as archive:
            return unpack_bagit_archive(
                args,
                archive.getnames,
                archive.getmember,
                archive.extract,
                ctx,
                heartbeat_callback,
            )
    elif archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            return unpack_bagit_archive(
                args,
                archive.namelist,
                archive.getinfo,
                archive.extract,
                ctx,
                heartbeat_callback,
            )
    elif archive_type == ".tgz" or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            return unpack_bagit_archive(
                args,
                archive.getnames,
                archive.getmember,
                archive.extract,
                ctx,
                heartbeat_callback,
            )
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def unpack_bagit_archive(
    args: dict,
    list_archive_files,
    get_archive_member,
    extract_archive_file,
    ctx: dict,
    heartbeat_callback,
) -> list:
    dst_id = args["destination"]["file_id"]
    dst_dir = f"/mnt/onedata/.__onedata__file_id__{dst_id}"

    archive_files = list_archive_files()

    bagit_dir = find_bagit_dir(archive_files)
    data_dir = f"{bagit_dir}/data/"

    uploaded_files = []

    for file_path in archive_files:
        is_directory = file_path.endswith("/")
        if file_path.startswith(data_dir) and (not is_directory):
            try:
                subpath = file_path[len(data_dir) :]
                file_archive_info = get_archive_member(file_path)

                # tarfile info object has name and size attributes, while zipfile has file_name and file_size
                if hasattr(file_archive_info, "name"):
                    file_archive_info.name = subpath
                    file_size = file_archive_info.size
                else:
                    file_archive_info.filename = subpath
                    file_size = file_archive_info.file_size

                dst_path = f"/mnt/onedata/.__onedata__file_id__{dst_id}/{subpath}"

                # extracting large files may last for a long time, therefore monitor thread with heartbeats is needed
                monitor_thread = threading.Thread(
                    target=monitor_unpack,
                    args=(
                        dst_path,
                        file_size,
                        ctx,
                        heartbeat_callback,
                    ),
                    daemon=True,
                )
                monitor_thread.start()

                extract_archive_file(file_archive_info, dst_dir)
                uploaded_files.append(dst_path)
            except Exception as ex:
                raise Exception(f"Unpacking file {file_path} failed due to: {str(ex)}")
    return uploaded_files


def find_bagit_dir(archive_files: list) -> str:
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == "bagit.txt":
            return dir_path


def monitor_unpack(file_path: str, file_size: int, ctx: dict, heartbeat_callback):
    heartbeat_interval = int(ctx["timeoutSeconds"])
    size = 0
    while size < file_size:
        time.sleep(heartbeat_interval // 2)
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat_callback()
            size = current_size
