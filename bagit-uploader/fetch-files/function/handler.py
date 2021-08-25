import json
import os.path
import time
import threading
from pathlib import Path
import multiprocessing

import requests
from xrootdpyfs import XRootDPyFS

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
    try:
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

            try:
                for backoff_sec in [0, 2, 5, 7]:
                    try:
                        # clear logs from previous retries
                        logs = []
                        if file_exists(path, size):
                            logs.append({
                                "severity": "info",
                                "file": path,
                                "status": f"File with expected size {size} Bytes already exists.",
                                "envs": dict(os.environ)
                            }),
                            break
                        download_file(url, size, path),
                        uploaded_files.append(path)
                        break
                    except Exception as ex:
                        time.sleep(backoff_sec)
                        if backoff_sec == 7:
                            raise ex
                log(f"downloaded url: {url} \n")

            except Exception as e:
                logs.append({
                    "severity": "error",
                    "file": path,
                    "status": str(e),
                    "envs": dict(os.environ)
                })

        error_logs = []
        for log_object in logs:
            if log_object["severity"] == "error":
                error_logs.append(log_object)
        if len(error_logs) >= 1:
            return json.dumps({
                "exception": {
                    "reason": error_logs
                }
            })

        return json.dumps({
            "uploadedFiles": uploaded_files,
            "logs": logs
        })
    except Exception as ex:
        return json.dumps({
            "exception": {
                "reason": [str(ex)]
            }
        })


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

    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    except Exception as ex:
        raise Exception(f"Failed to create dirs on path due to: {str(ex)}")

    if is_xrootd(file_url):

        if not xrootd_url_is_reachable(file_url):
            log(f"xrootd url unreachable: {file_url} \n")
            raise Exception(f"XrootD file address: {file_url} is unreachable")
        dir, source_file = os.path.split(file_url)
        fs = XRootDPyFS(dir)
        with fs.open(source_file) as source, open(file_path, 'wb') as destination:
            while True:
                data = source.read(BLOCK_SIZE_BYTES)
                if not data:
                    break
                destination.write(data)
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

    time.sleep(3)
    downloaded_file_size = Path(file_path).stat().st_size

    if downloaded_file_size != file_size:
        raise Exception(f"Downloaded file has wrong size (expected size has been taken from fetch.txt file). "
                        f"Expected: {file_size} B, Downloaded: {downloaded_file_size} B")

    return


# Some incorrect xrootd url cause xrdcp/xrdfs  methods to hang and last forever, until timeout is reached.
# Therefore dedicated timeout-based check is needed.
def xrootd_url_is_reachable(url: str) -> bool:
    p = multiprocessing.Process(target=stat_xrootd_file, args=(url,))
    p.start()
    p.join(5)
    if p.is_alive():
        p.terminate()
        return False
    return True


def stat_xrootd_file(file_url):
    dir, source_file = os.path.split(file_url)
    fs = XRootDPyFS(dir)
    with fs.open(source_file):
        pass

    # timeout = 5
    # parts = url.split("//")
    # command = ["xrdfs", f"{parts[0]}//{parts[1]}/", "stat", f"/{parts[2]}"]
    #
    # p = Popen(command, stdout=PIPE, stderr=PIPE)
    # for t in range(timeout):
    #     time.sleep(1)
    #     if p.poll() is not None:
    #         return True
    # p.kill()
    # return False


def log(log_entry):
    with open('/tmp/log.txt', 'a') as file:
        file.write(log_entry)


def file_exists(path: str, size: int) -> bool:
    if os.path.exists(path):
        if Path(path).stat().st_size == size:
            return True
        else:
            try:
                os.remove(path)
            except Exception as ex:
                raise Exception(f"Failed to delete existing file with unexpected size due to: {str(ex)}")

    return False
