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


def handle(req: bytes):
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

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    try:
        return json.dumps({"uploadedFiles": unpack_data_dir(args)})
    except:
        return json.dumps("FAILED")


def unpack_data_dir(args: dict):
    archive_filename = args["archive"]["name"]
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        with tarfile.TarFile(archive_path) as archive:
            return unpack_bagit_archive(args, archive.getnames, archive.getmember, archive.extract)
    elif archive_type == '.zip':
        with zipfile.ZipFile(archive_path) as archive:
            return unpack_bagit_archive(args, archive.namelist, archive.getinfo, archive.extract)
    elif archive_type == '.tgz' or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            return unpack_bagit_archive(args, archive.getnames, archive.getmember, archive.extract)


def unpack_bagit_archive(
        args: dict,
        list_archive_files: Callable[[], list],
        get_archive_member: Callable[[str], Union[tarfile.TarInfo, zipfile.ZipInfo]],
        extract_archive_file: Callable[[Union[tarfile.TarInfo, zipfile.ZipInfo]]]):
    dst_id = args["destination"]["file_id"]
    dst_dir = f'/mnt/onedata/.__onedata__file_id__{dst_id}'
    extracted_files = []

    archive_files = list_archive_files()

    bagit_dir = find_bagit_dir(archive_files)
    data_dir = f'{bagit_dir}/data/'

    for file_path in archive_files:
        if file_path.startswith(data_dir):
            try:
                subpath = file_path[len(data_dir):]
                file_archive_info = get_archive_member(file_path)

                # tarfile info object has name and size attributes, while zipfile has file_name and file_size
                if hasattr(file_archive_info, "name"):
                    file_archive_info.name = subpath
                    file_size = file_archive_info.size
                else:
                    file_archive_info.file_name = subpath
                    file_size = file_archive_info.file_size

                dst_path = f'/mnt/onedata/.__onedata__file_id__{dst_id}/{subpath}'

                # extracting large files may last for a long time, therefore monitor thread with heartbeats is needed
                monitor_thread = threading.Thread(target=monitor_unpack, args=(dst_path, file_size,), daemon=True)
                monitor_thread.start()

                extract_archive_file(file_archive_info, dst_dir)

                extracted_files.append(dst_path)
            except:
                pass
    return extracted_files


def find_bagit_dir(archive_files: list):
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
        time.sleep(HEARTBEAT_INTERVAL_SEC)
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat()
            size = current_size
