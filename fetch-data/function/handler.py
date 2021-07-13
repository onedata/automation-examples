import json
import requests


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    file_path = f'/mnt/onedata/.__onedata__file_id__{args["fetch_file"]["file_id"]}'
    target_dir_id = args["target_dir"]["file_id"]

    with open(file_path, "rb") as fd:
        line = fd.readline()
        while line:
            parts = line.decode("utf-8").strip().split(" ")
            url = parts[0]
            file_name = parts[2]
            r = requests.get(url, stream=True, allow_redirects=True)
            target_file_path = f'/mnt/onedata/.__onedata__file_id__{target_dir_id}/{file_name}'
            with open(target_file_path, 'wb') as f:
                for chunk in r.iter_content(32 * 1024):
                    f.write(chunk)
            line = fd.readline()

    return json.dumps({})
