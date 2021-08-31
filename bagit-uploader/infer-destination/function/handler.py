import json
import os
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def handle(req: bytes):
    """Given archive and its name, infers destination directory name and creates if necessary.

    Args Structure:
        archive (regular-file): input archive, expected name format: <id>+<record>+<timestamp>.tar/zip/tgz.
                                Takes <record> as destination name, and creates directory in the same place as archive.
    Return:
        destination (directory): destination directory
    """
    try:
        args = json.loads(req)
        archive = args["archive"]
        parent_id = archive["parent_id"]
        archive_name = archive["name"]
        access_token = args["credentials"]["accessToken"]
        host = args["credentials"]["host"]
        logs = []

        dst_name = infer_destination_name(archive_name)
        dst_path = f"/mnt/onedata/.__onedata__file_id__{parent_id}/{dst_name}"

        ensure_directory_exists(dst_path)
        dst_file_id = fetch_dir_file_id(access_token, host, parent_id, dst_name)
        dst_attrs = fetch_directory_attrs(access_token, host, dst_file_id)

        return json.dumps({
            "destination": dst_attrs,
            "logs": logs
        })
    except Exception as ex:
        return json.dumps({
            "exception": {
                "reason": f"Failed to infer destination due to: {str(ex)}"
            }})


def infer_destination_name(archive_name: str) -> str:
    try:
        _, record, _ = archive_name.split("+")
        return record
    except Exception as ex:
        raise Exception(
            f"Failed to infer destination name, due to wrong archive name. "
            f"Expected archive format: <id>+<record>+<timestamp>.tar/zip/tgz. "
            f"Error: {str(ex)}")


def ensure_directory_exists(dir_path: str):
    try:
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
    except Exception as ex:
        raise Exception(f"Failed to ensure destination exists and create if necessary. Error: {str(ex)}")


def fetch_dir_file_id(access_token: str, host: str, parent_id: str, dst_name: str):
    try:
        headers = {"X-Auth-Token": access_token}
        url = f'https://{host}/api/v3/oneprovider/data/{parent_id}/children'
        resp = requests.get(url, headers=headers, verify=False)
        if not resp.ok:
            raise Exception(
                f"Failed to fetch info about destination parent directory via REST. Code: {str(resp.status_code)}")
        root_content = resp.json()

        dst_dir_id = ""
        for item_info in root_content["children"]:
            if item_info["name"] == dst_name:
                dst_dir_id = item_info["id"]
        if dst_dir_id == "":
            raise Exception(f"Destination is not listed among root children. Unable to obtain destination file id.")
    except Exception as ex:
        raise Exception(f"Failed to collect info about destination parent directory, due to: {str(ex)}")


def fetch_directory_attrs(access_token: str, host: str, directory_file_id: str) -> dict:
    try:
        headers = {"X-Auth-Token": access_token}
        url = f'https://{host}/api/v3/oneprovider/data/{directory_file_id}'
        resp = requests.get(url, headers=headers, verify=False)
        if not resp.ok:
            raise Exception(f"Failed to get info about destination attrs via REST. Code: {str(resp.status_code)}")
        return resp.json()
    except Exception as ex:
        raise Exception(f"Failed to obtain destination file attrs, due to: {str(ex)}")
