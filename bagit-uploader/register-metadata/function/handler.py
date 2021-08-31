from collections.abc import Callable
import json
import os.path
import tarfile
from typing import IO
import time
import zipfile

import requests
import xattr

SUPPORTED_CHECKSUM_ALGORITHMS: tuple = ('md5', 'sha1', 'sha256', 'sha512', 'adler32')

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""


def handle(req: bytes) -> str:
    """Reads manifests from bagit archive and sets them as custom metadata for each file.
        Registers json metadata from *metadata.json files (does not include files from data/ directory)
        as json metadata for each file.

    Args Structure:
        heartbeatUrl (str): url where heartbeats are posted to, automatically added to lambda
        destination (dir-file): destination where all files has been extracted to before
        archive (regular-file): archive to process

    """
    global HEARTBEAT_URL

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    try:
        register_checksum_metadata(args)
    except Exception as ex:
        return json.dumps({
            "exception": f"Checksum metadata registration failed due to: {str(ex)}"
        })
    try:
        register_json_metadata(args)
    except Exception as ex:
        return json.dumps({
            "exception": f"JSON metadata registration failed due to: {str(ex)}"
        })

    return json.dumps({})


def register_checksum_metadata(args: dict):
    archive_filename = args["archive"]["name"]
    archive_name, archive_type = os.path.splitext(archive_filename)
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'

    if archive_type == '.tar':
        with tarfile.open(archive_path) as archive:
            return register_checksum_metadata_from_archive(dst_dir_path, archive.getnames, archive.extractfile)
    elif archive_type == '.zip':
        with zipfile.ZipFile(archive_path) as archive:
            return register_checksum_metadata_from_archive(dst_dir_path, archive.namelist, archive.open)
    elif archive_type == '.tgz' or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            return register_checksum_metadata_from_archive(dst_dir_path, archive.getnames, archive.extractfile)
    else:
        raise Exception(f"Archive format not supported: {archive_type}")


def register_checksum_metadata_from_archive(
        destination_dir_path: str,
        list_archive_files: Callable[[], list],
        open_archive_file: Callable[[str], IO[bytes]]
):
    file_paths = list_archive_files()
    root_dir = find_root_dir(file_paths)

    for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
        manifest_file = f'{root_dir}/manifest-{algorithm}.txt'
        if manifest_file in file_paths:
            manifest = open_archive_file(manifest_file)
            for line in manifest:
                heartbeat()
                try:
                    checksum, file_path = line.decode('utf-8').strip().split()
                except Exception as ex:
                    raise Exception(f"Failed to parse manifest line: {line.decode('utf-8')}")
                try:
                    append_xattr(file_path, checksum, algorithm, destination_dir_path)
                except Exception as ex:
                    raise Exception(f"Failed to set xattr metadata due to: {str(ex)}")


def register_json_metadata(args: dict):
    archive_filename = args["archive"]["name"]
    archive_name, archive_type = os.path.splitext(archive_filename)
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'

    if archive_type == '.tar':
        with tarfile.open(archive_path) as archive:
            return register_json_metadata_from_archive(dst_dir_path, archive.getnames, archive.extractfile)
    elif archive_type == '.zip':
        with zipfile.ZipFile(archive_path) as archive:
            return register_json_metadata_from_archive(dst_dir_path, archive.namelist, archive.open)
    elif archive_type == '.tgz' or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            return register_json_metadata_from_archive(dst_dir_path, archive.getnames, archive.extractfile)
    else:
        raise Exception(f"Archive format not supported: {archive_type}")


def register_json_metadata_from_archive(
        destination_dir_path: str,
        list_archive_files: Callable[[], list],
        open_archive_file: Callable[[str], IO[bytes]]
):
    file_paths = list_archive_files()
    for file_path in file_paths:
        if is_json_metadata_file(file_path):

            try:
                json_metadata_file = open_archive_file(file_path)
                json_metadata = json.loads(json_metadata_file.read())
            except Exception as ex:
                raise Exception(f"Failed to open and load json metadata from file {file_path} due to {str(ex)}")

            if "metadata" in json_metadata:
                # process file, there all metadata are stored under "metadata" key
                metadata_list = json_metadata["metadata"]
                for file_metadata in metadata_list:
                    try:
                        file_name = file_metadata["filename"].replace("data/", "")
                        file_path = f'{destination_dir_path}/{file_name}'
                        x = xattr.xattr(file_path)
                        current_metadata = {}
                        try:
                            current_metadata_str = x.get("onedata_json")
                            current_metadata = json.loads(current_metadata_str)
                        except:
                            pass
                        current_metadata.update(file_metadata)
                        x.set("onedata_json", str.encode(json.dumps(current_metadata)))
                    except:
                        pass
            else:
                # process file, there all metadata are stored under file path key
                try:
                    for file_path_metadata in json_metadata:
                        file_name = file_path_metadata.replace("data/", "")
                        file_path = f'{destination_dir_path}/{file_name}'
                        metadata = json_metadata[file_path_metadata]
                        x = xattr.xattr(file_path)
                        x.set("onedata_json", str.encode(json.dumps(metadata)))
                except:
                    pass


def append_xattr(file_path: str, checksum: str, algorithm: str, dst_dir_path: str):
    xattr_key = f'checksum.{algorithm}.expected'
    p = os.path.relpath(file_path, 'data/')
    file_new_path = f'{dst_dir_path}/{p}'
    x = xattr.xattr(file_new_path)
    try:
        x.set(xattr_key, str.encode(f"\"{checksum}\""))
    except Exception as ex:
        raise Exception(f"Failed to set xattr {xattr_key}:{checksum} on file {file_path} due to: {str(ex)}")


def find_root_dir(file_paths: list) -> str:
    for file_path in file_paths:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def is_json_metadata_file(file_path: str) -> bool:
    not_in_data_dir = not "/data/" in file_path
    is_metadata_name = "metadata.json" in file_path
    return is_metadata_name and not_in_data_dir


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
