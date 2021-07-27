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
    args = json.loads(req)
    archive = args["archive"]
    parent_id = archive["parent_id"]
    archive_name = archive["name"]
    accessToken = args["credentials"]["accessToken"]
    host = args["credentials"]["host"]

    # infer destination directory name, and create directory if does not exist
    id, rec, ts = archive_name.split("-")
    dst_path = f"/mnt/onedata/.__onedata__file_id__{parent_id}/{rec}"
    if not os.path.exists(dst_path):
        os.mkdir(dst_path)

    # get created parent directory content, to extract dst file_id
    headers = {"X-Auth-Token": accessToken}
    url1 = f'https://{host}/api/v3/oneprovider/data/{parent_id}/children'
    resp1 = requests.get(url1, headers=headers, verify=False)
    root_content = json.loads(resp1.text)

    dst_dir_id = ""
    for item_info in root_content["children"]:
        if item_info["name"] == rec:
            dst_dir_id = item_info["id"]

    # having destination file_id, get its file attributes
    url2 = f'https://{host}/api/v3/oneprovider/data/{dst_dir_id}'
    resp2 = requests.get(url2, headers=headers, verify=False)
    dst_attrs = json.loads(resp2.text)

    return json.dumps({"destination": dst_attrs})
