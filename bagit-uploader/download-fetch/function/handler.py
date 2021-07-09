import json
import requests
import time
import os


BLOCK_SIZE = 262144
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

    files = args["files_to_fetch"]

    uploaded_files = []

    for file_info in files:
        url = file_info["url"]
        size = file_info["size"]
        path = f'/mnt/onedata/{file_info["path"]}'

        os.makedirs(os.path.dirname(path), exist_ok=True)

        if is_xrootd(url):
            os.system(f"xrdcp {url} {path}")
            heartbeat()
        else:
            r = requests.get(url, stream=True, allow_redirects=True)
            with open(path, 'wb') as f:
                for chunk in r.iter_content(32 * 1024):
                    heartbeat()
                    f.write(chunk)
            uploaded_files.append(path)

    return json.dumps({"uploaded_files": uploaded_files})




def is_xrootd(url):
    url.startswith("root:/")


def heartbeat():
    global HEARTBEAT_URL, LAST_HEARTBEAT, HEARTBEAT_CYCLE
    if time.time() - LAST_HEARTBEAT > HEARTBEAT_CYCLE:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT = time.time()
