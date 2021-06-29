import json
import os.path
import tarfile


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    try:
        return json.dumps({"download": get_files_to_download(args)})
    except:
        return json.dumps("FAILED")


def get_files_to_download(args):
    archive_path = f'/mnt/onedata/.__onedata__file_id__{args["archive"]["file_id"]}' 

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
                    "path": f'.__onedata__file_id__{args["archive"]["parent_id"]}/{dst_path.lstrip("data/")}'
                })
            return files_to_download
        else:
            return []


def find_bagit_dir(archive_files):
    for file_path in archive_files:
        dir_path, file_name = os.path.split(file_path)
        if file_name == 'bagit.txt':
            return dir_path
