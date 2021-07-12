import json
import os.path
import tarfile
import zipfile
import time
import requests

HEARTBEAT_CYCLE = 150
LAST_HEARTBEAT = 0
HEARTBEAT_URL = ""


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    global HEARTBEAT_URL, LAST_HEARTBEAT

    args = json.loads(req)

    LAST_HEARTBEAT = time.time()
    HEARTBEAT_URL = args["heartbeatUrl"]

    try:
        return json.dumps({"filesToFetch": get_files_to_download(args)})
    except:
        return json.dumps("FAILED")


def get_files_to_download(args):
    archive_filename = args["archive"]["name"]
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        return get_files_to_fetch_from_tar_bagit_archive(args)
    elif archive_type == '.zip':
        return get_files_to_fetch_from_zip_bagit_archive(args)
    elif archive_type == '.tgz' or archive_type == ".gz":
        return get_files_to_fetch_from_tgz_bagit_archive(args)


def get_files_to_fetch_from_tar_bagit_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_id = args["destination"]["file_id"]

    with tarfile.TarFile(archive_path) as archive:
        archive_files = archive.getnames()

        bagit_dir = find_bagit_dir(archive_files)
        fetch_file = f'{bagit_dir}/fetch.txt'

        if fetch_file in archive_files:
            files_to_download = []
            for line in archive.extractfile(fetch_file):
                url, size, dst_path = line.decode('utf-8').strip().split()
                files_to_download.append({
                    "url": url,
                    "size": int(size),
                    "path": f'.__onedata__file_id__{dst_id}/{dst_path[len("data/"):]}'
                })
                heartbeat()
            return files_to_download
        else:
            return []


def get_files_to_fetch_from_zip_bagit_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_id = args["destination"]["file_id"]

    with zipfile.ZipFile(archive_path) as archive:
        archive_files = archive.namelist()

        bagit_dir = find_bagit_dir(archive_files)
        fetch_file = f'{bagit_dir}/fetch.txt'

        if fetch_file in archive_files:
            files_to_download = []
            with archive.open(fetch_file) as ff:
                for line in ff:
                    url, size, dst_path = line.decode('utf-8').strip().split()
                    files_to_download.append({
                        "url": url,
                        "size": int(size),
                        "path": f'.__onedata__file_id__{dst_id}/{dst_path[len("data/"):]}'
                    })
                    heartbeat()
            return files_to_download
        else:
            return []


def get_files_to_fetch_from_tgz_bagit_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_id = args["destination"]["file_id"]

    with tarfile.open(archive_path, "r:gz") as archive:
        archive_files = archive.getnames()

        bagit_dir = find_bagit_dir(archive_files)
        fetch_file = f'{bagit_dir}/fetch.txt'

        if fetch_file in archive_files:
            files_to_download = []
            for line in archive.extractfile(fetch_file):
                url, size, dst_path = line.decode('utf-8').strip().split()
                files_to_download.append({
                    "url": url,
                    "size": int(size),
                    "path": f'.__onedata__file_id__{dst_id}/{dst_path[len("data/"):]}'
                })
                heartbeat()
            return files_to_download
        else:
            return []


def find_bagit_dir(archive_files):
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def heartbeat():
    global HEARTBEAT_URL, LAST_HEARTBEAT, HEARTBEAT_CYCLE
    if time.time() - LAST_HEARTBEAT > HEARTBEAT_CYCLE:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT = time.time()
