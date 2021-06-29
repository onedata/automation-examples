import hashlib
import json
import os.path
import tarfile


SUPPORTED_CHECKSUM_ALGORITHMS = ('md5', 'sha1', 'sha256', 'sha512')

BLOCK_SIZE = 262144


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    valid_bagit_archives = []

    for archive in args["archives"]:
        try:
            assert_valid_bagit_archive(f'/mnt/onedata/.__onedata__file_id__{archive["file_id"]}')
        except:
            continue
        else:
            valid_bagit_archives.append(archive)

    return json.dumps({"validBagitArchives": valid_bagit_archives})


def assert_valid_bagit_archive(archive_path):
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


def find_bagit_dir(archive_files):
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def calculate_checksum(fd, algorithm):
    checksum = getattr(hashlib, algorithm)()

    while True:
        data = fd.read(BLOCK_SIZE)
        if not data:
            break
        checksum.update(data)

    return checksum.hexdigest()
