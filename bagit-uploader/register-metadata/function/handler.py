import json
import os
import tarfile
import xattr
import zipfile

SUPPORTED_CHECKSUM_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'adler32']


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)
    register_checksum_metadata(args)
    # register_json_metadata(archive_path, archive_parent_id)
    return json.dumps({})


def register_checksum_metadata(args):
    archive_filename = args["archive"]["name"]
    archive_name, archive_type = os.path.splitext(archive_filename)

    if archive_type == '.tar':
        return register_checksum_metadata_tar_archive(args)
    elif archive_type == '.zip':
        return register_checksum_metadata_zip_archive(args)
    elif archive_type == '.tgz' or archive_type == ".gz":
        return register_checksum_metadata_tgz_archive(args)


def register_checksum_metadata_tar_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'
    with tarfile.open(archive_path) as archive:
        file_paths = archive.getnames()
        root_dir = find_root_dir(file_paths)

        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            manifest_file = f'{root_dir}/manifest-{algorithm}.txt'
            if manifest_file in file_paths:
                manifest = archive.extractfile(manifest_file)
                for line in manifest:
                    checksum, file_path = line.decode('utf-8').strip().split()
                    append_xattr(file_path, checksum, algorithm, dst_dir_path)


def register_checksum_metadata_zip_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'

    with zipfile.ZipFile(archive_path) as archive:
        file_paths = archive.namelist()
        root_dir = find_root_dir(file_paths)

        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            manifest_file = f'{root_dir}/manifest-{algorithm}.txt'
            if manifest_file in file_paths:
                with archive.open(manifest_file) as mf:
                    for line in mf:
                        checksum, file_path = line.decode('utf-8').strip().split()
                        append_xattr(file_path, checksum, algorithm, dst_dir_path)


def register_checksum_metadata_tgz_archive(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}'
    dst_dir_path = f'/mnt/onedata/.__onedata__file_id__{args["destination"]["file_id"]}'
    with tarfile.open(archive_path, "r:gz") as archive:
        file_paths = archive.getnames()
        root_dir = find_root_dir(file_paths)

        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            manifest_file = f'{root_dir}/manifest-{algorithm}.txt'
            if manifest_file in file_paths:
                manifest = archive.extractfile(manifest_file)
                for line in manifest:
                    checksum, file_path = line.decode('utf-8').strip().split()
                    append_xattr(file_path, checksum, algorithm, dst_dir_path)


def register_json_metadata(archive_path, archive_parent_id):
    with tarfile.open(archive_path) as archive:
        file_paths = archive.getnames()
        root_dir = find_root_dir(file_paths)
        json_metadata_file_path = f'{root_dir}/metadata.json'
        json_metadata_file = archive.extractfile(json_metadata_file_path)
        json_metadata = json.loads(json_metadata_file.read())
        metadata_list = json_metadata["metadata"]
        for file_metadata in metadata_list:
            file_name = file_metadata["filename"]
            file_path = f'/mnt/onedata/{archive_parent_id}/{remove_prefix("data/", file_name)}'
            x = xattr.xattr(file_path)
            x.set("onedata_json", json.dumps(file_metadata))


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
