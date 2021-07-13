import json
import os
import tarfile
import xattr
import zipfile
import time
import requests

SUPPORTED_CHECKSUM_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'adler32']

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

    register_json_metadata(args)

    return json.dumps({})


def register_json_metadata(args):
    archive_filename = args["archive"]["name"]
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        register_json_metadata_tar_archive(args)
    elif archive_type == '.zip':
        register_json_metadata_zip_archive(args)
    elif archive_type == '.tgz' or archive_type == ".gz":
        register_json_metadata_tgz_archive(args)


def register_json_metadata_tar_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'
    with tarfile.open(archive_path) as archive:
        file_paths = archive.getnames()
        for file_path in file_paths:
            if is_json_metadata_file(file_path):
                json_metadata_file = archive.extractfile(file_path)
                json_metadata = json.loads(json_metadata_file.read())
                metadata_list = json_metadata["metadata"]
                for file_metadata in metadata_list:
                    try:
                        file_name = file_metadata["filename"].replace("data/", "")
                        file_path = f'{dst_dir_path}/{file_name}'
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


def register_json_metadata_zip_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'
    with zipfile.ZipFile(archive_path) as archive:
        file_paths = archive.namelist()
        for file_path in file_paths:
            if is_json_metadata_file(file_path):
                with archive.open(file_path) as fd:
                    json_metadata = json.loads(fd.read())
                    metadata_list = json_metadata["metadata"]
                    for file_metadata in metadata_list:
                        try:
                            file_name = file_metadata["filename"].replace("data/", "")
                            file_path = f'{dst_dir_path}/{file_name}'
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


def register_json_metadata_tgz_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'
    with tarfile.open(archive_path, "r:gz") as archive:
        file_paths = archive.getnames()
        for file_path in file_paths:
            if is_json_metadata_file(file_path):
                json_metadata_file = archive.extractfile(file_path)
                json_metadata = json.loads(json_metadata_file.read())
                metadata_list = json_metadata["metadata"]
                for file_metadata in metadata_list:
                    try:
                        file_name = file_metadata["filename"].replace("data/", "")
                        file_path = f'{dst_dir_path}/{file_name}'
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


def append_xattr(file_path, checksum, algorithm, dst_dir_path):
    xattr_key = f'checksum.{algorithm}.expected'
    p = os.path.relpath(file_path, 'data/')
    file_new_path = f'{dst_dir_path}/{p}'
    x = xattr.xattr(file_new_path)
    try:
        x.set(xattr_key, str.encode(checksum))
    except:
        pass


def find_root_dir(file_paths):
    for file_path in file_paths:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]


def is_json_metadata_file(file_path):
    is_in_data_dir = "/data/" in file_path
    is_metadata_name = "metadata.json" in file_path
    return is_metadata_name and is_in_data_dir


def heartbeat():
    global HEARTBEAT_URL, LAST_HEARTBEAT, HEARTBEAT_CYCLE
    if time.time() - LAST_HEARTBEAT > HEARTBEAT_CYCLE:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT = time.time()
