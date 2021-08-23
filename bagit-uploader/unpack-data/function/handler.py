from collections.abc import Callable
import json
import os.path
import tarfile
import threading
from typing import Union
import time
from pathlib import Path
import zipfile

import requests

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""

UPLOADED_FILES = []


def handle(req: bytes) -> str:
    """Unpack files from archive /data directory and puts them under destination directory.

    Args Structure:
        heartbeatUrl (str): url where heartbeats are posted to, automatically added to lambda
        destination (dir-file): destination where all files will be extracted to
        archive (regular-file): archive to unpack data from

    Return:
        uploadedFiles (batch/list of strings): list of file paths, which were successfully extracted
            into destination directory.
    """
    global HEARTBEAT_URL
    global UPLOADED_FILES

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    try:
        unpack_data_dir(args)
        return json.dumps({
            "uploadedFiles": UPLOADED_FILES,
            "logs": [{
                "severity": "info",
                "file": args["archive"]["name"],
                "status": f"Successfully uploaded {str(len(UPLOADED_FILES))} files."
            }]
        })
    except Exception as ex:
        return json.dumps({
            "uploadedFiles": UPLOADED_FILES,
            "logs": [{
                "severity": "error",
                "file": args["archive"]["name"],
                "status": str(ex)
            }]
        })


def unpack_data_dir(args: dict):
    archive_filename = args["archive"]["name"]
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        with tarfile.TarFile(archive_path) as archive:
            unpack_bagit_archive(args, archive.getnames, archive.getmember, archive.extract)
    elif archive_type == '.zip':
        with zipfile.ZipFile(archive_path) as archive:
            unpack_bagit_archive(args, archive.namelist, archive.getinfo, archive.extract)
    elif archive_type == '.tgz' or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            unpack_bagit_archive(args, archive.getnames, archive.getmember, archive.extract)
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def unpack_bagit_archive(
        args: dict,
        list_archive_files: Callable[[], list],
        get_archive_member: Callable[[str], Union[tarfile.TarInfo, zipfile.ZipInfo]],
        extract_archive_file: Callable[[Union[tarfile.TarInfo, zipfile.ZipInfo]]]
):
    global UPLOADED_FILES
    dst_id = args["destination"]["file_id"]
    dst_dir = f'/mnt/onedata/.__onedata__file_id__{dst_id}'

    archive_files = list_archive_files()

    bagit_dir = find_bagit_dir(archive_files)
    data_dir = f'{bagit_dir}/data/'

    for file_path in archive_files:
        is_directory = file_path.endswith("/")
        if file_path.startswith(data_dir) and (not is_directory):
            try:
                subpath = file_path[len(data_dir):]
                file_archive_info = get_archive_member(file_path)

                # tarfile info object has name and size attributes, while zipfile has file_name and file_size
                if hasattr(file_archive_info, "name"):
                    file_archive_info.name = subpath
                    file_size = file_archive_info.size
                else:
                    file_archive_info.filename = subpath
                    file_size = file_archive_info.file_size

                dst_path = f'/mnt/onedata/.__onedata__file_id__{dst_id}/{subpath}'

                # extracting large files may last for a long time, therefore monitor thread with heartbeats is needed
                monitor_thread = threading.Thread(target=monitor_unpack, args=(dst_path, file_size,), daemon=True)
                monitor_thread.start()

                extract_archive_file(file_archive_info, dst_dir)
                UPLOADED_FILES.append(dst_path)
            except Exception as ex:
                raise Exception(f"Unpacking file {file_path} failed due to: {str(ex)}")


def find_bagit_dir(archive_files: list) -> str:
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time


def monitor_unpack(file_path: str, file_size: int):
    size = 0
    while size < file_size:
        time.sleep(HEARTBEAT_INTERVAL_SEC // 2)
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat()
            size = current_size
