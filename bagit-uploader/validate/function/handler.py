import hashlib
import json
import os.path
import tarfile
import zipfile
import re
import zlib
import time
import requests

SUPPORTED_CHECKSUM_ALGORITHMS = ('md5', 'sha1', 'sha256', 'sha512', 'adler32')

BLOCK_SIZE = 262144

HEARTBEAT_CYCLE = 150
LAST_HEARTBEAT_TIME = 0
HEARTBEAT_URL = ""


def handle(req: bytes):
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


def assert_valid_bagit_archive(archive_path, archive_filename):
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        assert_valid_tar_bagit_archive(archive_path)
    elif archive_type == '.zip':
        assert_valid_zip_bagit_archive(archive_path)
    elif archive_type == '.tgz' or archive_type == ".gz":
        assert_valid_tgz_bagit_archive(archive_path)


def assert_valid_tar_bagit_archive(archive_path):
    with tarfile.TarFile(archive_path) as archive:
        archive_files = archive.getnames()

        bagit_dir = find_bagit_dir(archive_files)
        assert bagit_dir is not None

        data_dir = f'{bagit_dir}/data'
        fetch_file = f'{bagit_dir}/fetch.txt'
        assert (data_dir in archive_files) or (fetch_file in archive_files)

        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            tagmanifest_file = f'{bagit_dir}/tagmanifest-{algorithm}.txt'

            if tagmanifest_file not in archive_files:
                continue

            for line in archive.extractfile(tagmanifest_file):
                exp_checksum, file_rel_path = line.decode('utf-8').strip().split()
                fd = archive.extractfile(f'{bagit_dir}/{file_rel_path}')
                assert exp_checksum == calculate_checksum(fd, algorithm)


def assert_valid_zip_bagit_archive(archive_path):
    with zipfile.ZipFile(archive_path) as archive:
        archive_files = archive.namelist()

        bagit_dir = find_bagit_dir(archive_files)
        assert bagit_dir is not None

        data_dir = f'{bagit_dir}/data'
        fetch_file = f'{bagit_dir}/fetch.txt'
        assert (data_dir in archive_files) or (fetch_file in archive_files)

        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            tagmanifest_file = f'{bagit_dir}/tagmanifest-{algorithm}.txt'

            if tagmanifest_file not in archive_files:
                continue
            with archive.open(tagmanifest_file) as tmf:
                for line in tmf:
                    exp_checksum, file_rel_path = line.decode('utf-8').strip().split()
                    fd = archive.open(f'{bagit_dir}/{file_rel_path}')
                    assert exp_checksum == calculate_checksum(fd, algorithm)


def assert_valid_tgz_bagit_archive(archive_path):
    with tarfile.open(archive_path, "r:gz") as archive:
        archive_files = archive.getnames()

        bagit_dir = find_bagit_dir(archive_files)
        assert bagit_dir is not None

        data_dir = f'{bagit_dir}/data'
        fetch_file = f'{bagit_dir}/fetch.txt'
        assert (data_dir in archive_files) or (fetch_file in archive_files)

        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            tagmanifest_file = f'{bagit_dir}/tagmanifest-{algorithm}.txt'

            if tagmanifest_file not in archive_files:
                continue

            for line in archive.extractfile(tagmanifest_file):
                exp_checksum, file_rel_path = line.decode('utf-8').strip().split()
                fd = archive.extractfile(f'{bagit_dir}/{file_rel_path}')
                assert exp_checksum == calculate_checksum(fd, algorithm)


def find_bagit_dir(archive_files):
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def calculate_checksum(fd, algorithm):
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


def assert_proper_bagit_txt_content(fd):
    content = fd.readlines()
    for index, line in enumerate(content):
        key = line.decode("utf-8").trim().split(":")[0]
        if key == "BagIt-Version":
            bagit_version = line.decode("utf-8").trim().split(":")[1]
            assert_correct_bagit_version(bagit_version)
            next_line_key = content[index + 1].decode("utf-8").trim().split(":")[0]
            next_line_value = content[index + 1].decode("utf-8").trim().split(":")[1]
            assert next_line_key == "Tag-File-Character-Encoding"
            assert next_line_value != ""


def assert_correct_bagit_version(bagit_version):
    match = re.match("^[0-9]+.[0-9]+$", bagit_version)
    assert bool(match)


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > 150:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT_TIME = current_time
