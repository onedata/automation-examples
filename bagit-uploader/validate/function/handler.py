from collections.abc import Callable
import hashlib
import json
import os.path
import re
import tarfile
from typing import IO
import time
import zipfile
import zlib

import requests

SUPPORTED_CHECKSUM_ALGORITHMS: tuple = ('md5', 'sha1', 'sha256', 'sha512', 'adler32')
BLOCK_SIZE: int = 262144

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""


def handle(req: bytes) -> str:
    """Validates bagit archives and returns correct ones.

    Args Structure:
        heartbeatUrl (str): url where heartbeats are posted to, automatically added to lambda
        archives (batch/list of any-files): list of archives to process

    Return:
        validBagitArchives (batch/list of reg-files): list of regular-file validated bagit archives
    """
    global HEARTBEAT_URL

    args = json.loads(req)

    HEARTBEAT_URL = args["ctx"]["heartbeatUrl"]
    heartbeat()

    results = [process_item(item) for item in args["argsBatch"]]
    return json.dumps({"resultsBatch": results})


def process_item(args):
    logs = []
    archive = args["archive"]
    archive_filename = archive["name"]
    try:
        if not archive["type"] == "REG":
            raise Exception(f"File {archive_filename} is not a regular file")
        assert_valid_bagit_archive(f'/mnt/onedata/.__onedata__file_id__{archive["file_id"]}', archive_filename)
    except Exception as ex:
        return {"exception": {"file": archive_filename, "error": str(ex)}}
    return {
        "validBagitArchive": archive,
        "logs": logs
    }


def assert_valid_bagit_archive(archive_path: str, archive_filename: str):
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        with tarfile.open(archive_path) as archive:
            assert_valid_archive(archive.getnames, archive.extractfile)
    elif archive_type == '.zip':
        with zipfile.ZipFile(archive_path) as archive:
            assert_valid_archive(archive.namelist, archive.open)
    elif archive_type == '.tgz' or archive_type == ".gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            assert_valid_archive(archive.getnames, archive.extractfile)
    else:
        raise Exception(f"Unsupported archive type: {archive_type}")


def assert_valid_archive(
        list_archive_files: Callable[[], list],
        open_archive_file: Callable[[str], IO[bytes]]
):
    archive_files = list_archive_files()
    bagit_dir = find_bagit_dir(archive_files)
    if bagit_dir is None:
        raise Exception("Could not find bagit dir")

    with open_archive_file(f'{bagit_dir}/bagit.txt') as f:
        assert_proper_bagit_txt_content(f)

    data_dir = f'{bagit_dir}/data'
    fetch_file = f'{bagit_dir}/fetch.txt'
    if not ((data_dir in archive_files) or (fetch_file in archive_files) or (f'{bagit_dir}/data/' in archive_files)):
        raise Exception(
            f"Could not find fetch.txt file or /data directory inside bagit dir: {bagit_dir}. Found files: {str(archive_files)}")

    for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
        tagmanifest_file = f'{bagit_dir}/tagmanifest-{algorithm}.txt'

        if tagmanifest_file in archive_files:
            for line in open_archive_file(tagmanifest_file):
                exp_checksum, file_rel_path = line.decode('utf-8').strip().split()
                fd = open_archive_file(f'{bagit_dir}/{file_rel_path}')
                calculated_checksum = calculate_checksum(fd, algorithm)
                if not exp_checksum == calculated_checksum:
                    raise Exception(f"{algorithm} checksum verification failed for file {file_rel_path}. \n"
                                    f"Expected: {exp_checksum}, Calculated: {calculated_checksum}")


def find_bagit_dir(archive_files: list) -> str:
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def calculate_checksum(fd, algorithm: str) -> str:
    if algorithm == "adler32":
        checksum = 1
        while True:
            heartbeat()
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum = zlib.adler32(data, checksum)
        return format(checksum, 'x')
    else:
        checksum = getattr(hashlib, algorithm)()
        while True:
            heartbeat()
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum.update(data)
        return checksum.hexdigest()


def assert_proper_bagit_txt_content(fd: IO[bytes]):
    line1, line2 = fd.readlines()
    if not re.match(r"^\s*BagIt-Version: [0-9]+.[0-9]+\s*$", line1.decode("utf-8")):
        raise Exception(f"File bagit.txt has incorrect 1st line: {line1}")
    if not re.match(r"^\s*Tag-File-Character-Encoding: \w+", line2.decode("utf-8")):
        raise Exception(f"File bagit.txt has incorrect 2nd line: {line2}")


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
