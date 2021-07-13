import json
import requests
import time
import os
from pathlib import Path
import threading

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

    files = args["filesToFetch"]

    uploaded_files = []

    # time.sleep(2000)

    for file_info in files:
        url = file_info["url"]
        size = file_info["size"]
        path = f'/mnt/onedata/{file_info["path"]}'

        os.makedirs(os.path.dirname(path), exist_ok=True)

        if is_xrootd(url):
            x = threading.Thread(target=monitor_download, args=(path, size))
            x.start()
            os.system(f"xrdcp {url} {path}")
            heartbeat()
        else:
            r = requests.get(url, stream=True, allow_redirects=True)
            with open(path, 'wb') as f:
                for chunk in r.iter_content(32 * 1024):
                    heartbeat()
                    f.write(chunk)
            uploaded_files.append(path)

    return json.dumps({"uploadedFiles": uploaded_files})


def is_xrootd(url):
    url.startswith("root:/")


def heartbeat():
    global HEARTBEAT_URL, LAST_HEARTBEAT, HEARTBEAT_CYCLE
    if time.time() - LAST_HEARTBEAT > HEARTBEAT_CYCLE:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT = time.time()


def monitor_download(file_path, file_size):
    size = 0
    while size < file_size:
        time.sleep(HEARTBEAT_CYCLE)
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat()
            size = current_size
    return
