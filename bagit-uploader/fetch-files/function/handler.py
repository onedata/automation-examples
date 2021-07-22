import json
import os.path
import time
import threading
from pathlib import Path

import requests

BLOCK_SIZE: int = 262144

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""


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

    fetch_threads = {}
    uploaded_files = []

    for file_info in files:
        url = file_info["url"]
        size = file_info["size"]
        path = f'/mnt/onedata/{file_info["path"]}'

        os.makedirs(os.path.dirname(path), exist_ok=True)

        fetch_thread = threading.Thread(target=download_file, args=(url, size, path))
        fetch_thread.start()
        fetch_threads[path] = fetch_thread

    for path in fetch_threads:
        fetch_threads[path].join()
        uploaded_files.append(path)

    return json.dumps({"uploadedFiles": uploaded_files})


def is_xrootd(url: str):
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
        time.sleep(int(HEARTBEAT_INTERVAL_SEC / 2))
        current_size = Path(file_path).stat().st_size
        if current_size > size:
            heartbeat()
            size = current_size


def download_file(file_url: str, file_size: int, file_path: str):
    monitor_thread = threading.Thread(target=monitor_download, args=(file_path, file_size,), daemon=True)
    monitor_thread.start()

    if is_xrootd(file_url):
        os.system(f"xrdcp {file_url} {file_path}")
    else:
        r = requests.get(file_url, stream=True, allow_redirects=True)
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(32 * 1024):
                f.write(chunk)
    return
