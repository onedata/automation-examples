import json
import os
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    try:
        args = json.loads(req)
        archive = args["archive"]
        parent_id = archive["parent_id"]
        archive_name = archive["name"]
        accessToken = args["credentials"]["accessToken"]
        host = args["credentials"]["host"]
        logs = []

        # infer destination directory name, and create directory if does not exist
        try:
            id, rec, ts = archive_name.split("+")
        except Exception as ex:
            raise Exception(
                f"Failed to infer destination name, due to wrong archive name. "
                f"Expected archive format: <id>+<record>+<timestamp>.tar/zip/tgz. "
                f"Error: {str(ex)}")

        dst_path = f"/mnt/onedata/.__onedata__file_id__{parent_id}/{rec}"

        try:
            if not os.path.exists(dst_path):
                os.mkdir(dst_path)
        except Exception as ex:
            raise Exception(f"Failed to ensure destination exists and create if necessary. Error: {str(ex)}")

        # get created parent directory content, to extract dst file_id
        try:
            headers = {"X-Auth-Token": accessToken}
            url1 = f'https://{host}/api/v3/oneprovider/data/{parent_id}/children'
            resp1 = requests.get(url1, headers=headers, verify=False)
            if not resp1.ok:
                raise Exception(
                    f"Failed to fetch info about destination parent directory via REST. Code: {str(resp1.status_code)}")
            root_content = json.loads(resp1.text)

            dst_dir_id = ""
            for item_info in root_content["children"]:
                if item_info["name"] == rec:
                    dst_dir_id = item_info["id"]
            if dst_dir_id == "":
                raise Exception(f"Destination is not listed among root children. Unable to obtain destination file id.")
        except Exception as ex:
            raise Exception(f"Failed to collect info about destination parent directory, due to: {str(ex)}")

        # having destination file_id, get its file attributes
        try:
            url2 = f'https://{host}/api/v3/oneprovider/data/{dst_dir_id}'
            resp2 = requests.get(url2, headers=headers, verify=False)
            if not resp2.ok:
                raise Exception(f"Failed to get info about destination attrs via REST. Code: {str(resp2.status_code)}")
            dst_attrs = json.loads(resp2.text)
        except Exception as ex:
            raise Exception(f"Failed to obtain destination file attrs, due to: {str(ex)}")

        return json.dumps({
            "destination": dst_attrs,
            "logs": logs
        })
    except Exception as ex:
        return json.dumps({
            "exception": {
                "reason": f"Failed to infer destination due to: {str(ex)}"
            }})
