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

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    valid_bagit_archives = []

    for archive in args["archives"]:
        try:
            assert archive["type"] == "REG"
            archive_filename = archive["name"]
            assert_valid_bagit_archive(f'/mnt/onedata/.__onedata__file_id__{archive["file_id"]}', archive_filename)
        except:
            continue
        else:
            valid_bagit_archives.append(archive)

    return json.dumps({"validBagitArchives": valid_bagit_archives})


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


def assert_valid_archive(
        list_archive_files: Callable[[], list],
        open_archive_file: Callable[[str], IO[bytes]]
):
    archive_files = list_archive_files()
    bagit_dir = find_bagit_dir(archive_files)
    assert bagit_dir is not None

    with open_archive_file(f'{bagit_dir}/bagit.txt') as f:
        assert_proper_bagit_txt_content(f)

    data_dir = f'{bagit_dir}/data'
    fetch_file = f'{bagit_dir}/fetch.txt'
    assert (data_dir in archive_files) or (fetch_file in archive_files) or (f'{bagit_dir}/data/' in archive_files)

    for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
        tagmanifest_file = f'{bagit_dir}/tagmanifest-{algorithm}.txt'

        if tagmanifest_file not in archive_files:
            continue

        for line in open_archive_file(tagmanifest_file):
            exp_checksum, file_rel_path = line.decode('utf-8').strip().split()
            fd = open_archive_file(f'{bagit_dir}/{file_rel_path}')
            assert exp_checksum == calculate_checksum(fd, algorithm)


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
    assert re.match(r"^\s*BagIt-Version: [0-9]+.[0-9]+\s*$", line1.decode("utf-8"))
    assert re.match(r"^\s*Tag-File-Character-Encoding: \w+", line2.decode("utf-8"))


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
