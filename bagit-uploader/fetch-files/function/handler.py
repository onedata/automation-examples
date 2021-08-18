import json
import os.path
import time
import threading
from pathlib import Path
from subprocess import Popen, PIPE

import requests

BLOCK_SIZE_BYTES: int = 31457280

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
    logs = []

    envs = '\n'.join([f'{k}: {v}' for k, v in sorted(os.environ.items())])
    log(envs)


    for file_info in files:
        url = file_info["url"]
        size = file_info["size"]
        path = f'/mnt/onedata/{file_info["path"]}'

        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            #log(f"trying to download from url: {url} \n")
            download_file(url, size, path),
            log(f"downloaded url: {url} \n")
            uploaded_files.append(path)
            # logs.append({
            #     "severity": "info",
            #     "file": path,
            #     "status": "file fetched successfully"
            # })
        except Exception as e:
            logs.append({
                "severity": "error",
                "file": path,
                "status": str(e),
                "envs": dict(os.environ)
            })
    return json.dumps({"uploadedFiles": uploaded_files, "logs": logs})


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


def download_file(file_url: str, file_size: int, file_path: str):
    monitor_thread = threading.Thread(target=monitor_download, args=(file_path, file_size,), daemon=True)
    monitor_thread.start()

    if is_xrootd(file_url):
        if not xrootd_url_is_reachable(file_url):
            log(f"xrootd url unreachable: {file_url} \n")
            raise Exception(f"XrootD file address: {file_url} is unreachable")
        os.system(f"xrdcp {file_url} {file_path} {IGNORE_OUTPUT}")
    else:
        r = requests.get(file_url, stream=True, allow_redirects=True)
        if not r.ok:
            log(f"HTTP/S file address: {file_url} is unreachable. Code: {str(r.status_code)}")
            raise Exception(f"HTTP/S file address: {file_url} is unreachable. Code: {str(r.status_code)}")
        try:
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(BLOCK_SIZE_BYTES):
                    f.write(chunk)
        except Exception as e:
            log(f"Failed to write data to file due to: {str(e)}")
            raise Exception(f"Failed to write data to file due to: {str(e)}")

    return


# Some incorrect xrootd url cause xrdcp/xrdfs  methods to hang and last forever, until timeout is reached.
# Therefore dedicated timeout-based check is needed.
def xrootd_url_is_reachable(url: str) -> bool:
    timeout = 10
    parts = url.split("//")
    command = ["xrdfs", f"{parts[0]}//{parts[1]}/", "stat", f"/{parts[2]}"]

    p = Popen(command, stdout=PIPE, stderr=PIPE)
    for t in range(timeout):
        time.sleep(1)
        if p.poll() is not None:
            return True
    p.kill()
    return False


def log(log_entry):
    with open('/tmp/log.txt', 'a') as file:
        file.write(log_entry)
