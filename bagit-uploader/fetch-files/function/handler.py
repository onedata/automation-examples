import json
import requests
import time
import os
from pathlib import Path
import threading

BLOCK_SIZE = 262144
HEARTBEAT_CYCLE = 150
LAST_HEARTBEAT_TIME = 0
HEARTBEAT_URL = ""


def handle(req: bytes):
    """Downloads files to be fetched and puts them under given path

    Args Structure:
        heartbeatUrl (str): url where heartbeats are posted to, automatically added to lambda
        filesToFetch (batch/list of objects): informations about files to be fetched,
            using format:{"url": <str>, "size": <int>, "path": <str>}

    Return:
        uploadedFiles (batch/list of strings): list of file paths, which were successfully fetched and
            placed under given path.
    """
    global HEARTBEAT_URL

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    files = args["filesToFetch"]

    uploaded_files = []

    for file_info in files:
        url = file_info["url"]
        size = file_info["size"]
        path = f'/mnt/onedata/{file_info["path"]}'

        os.makedirs(os.path.dirname(path), exist_ok=True)

        monitor_thread = threading.Thread(target=monitor_download, args=(path, size,), daemon=True)
        monitor_thread.start()

        if is_xrootd(url):
            os.system(f"xrdcp {url} {path}")
            uploaded_files.append(path)
        else:
            r = requests.get(url, stream=True, allow_redirects=True)
            with open(path, 'wb') as f:
                for chunk in r.iter_content(32 * 1024):
                    f.write(chunk)
            uploaded_files.append(path)

    return json.dumps({"uploadedFiles": uploaded_files})


def is_xrootd(url):
    return url.startswith("root:/")


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_CYCLE:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT_TIME = current_time


def monitor_download(file_path, file_size):
    size = 0
    while size < file_size:
        time.sleep(HEARTBEAT_CYCLE)
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat()
            size = current_size
