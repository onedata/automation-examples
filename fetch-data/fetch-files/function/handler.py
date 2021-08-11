import json
import os.path
import time
import threading
from pathlib import Path

import requests

BLOCK_SIZE_BYTES: int = 30000000

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""

IGNORE_OUTPUT: str = "> /dev/null 2>&1"


def handle(req: bytes) -> str:
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
        file_url = file_info["url"]
        file_size = file_info["size"]
        file_path = f'/mnt/onedata/{file_info["path"]}'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        monitor_thread = threading.Thread(target=monitor_download, args=(file_path, file_size,), daemon=True)
        monitor_thread.start()

        if is_xrootd(file_url):
            os.system(f"xrdcp --silent {file_url} {file_path} {IGNORE_OUTPUT}")
        else:
            r = requests.get(file_url, stream=True, allow_redirects=True)
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(BLOCK_SIZE_BYTES):
                    f.write(chunk)
        uploaded_files.append(file_path)

    return json.dumps({"uploadedFiles": uploaded_files})


def is_xrootd(url: str) -> bool:
    return url.startswith("root:/")


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time


def monitor_download(file_path: str, file_size: int):
    size = 0
    while size < file_size:
        time.sleep(HEARTBEAT_INTERVAL_SEC // 2)
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat()
            size = current_size
