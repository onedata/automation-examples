import concurrent.futures
import json
import os.path
import time
from pathlib import Path

import requests
from XRootD import client

BLOCK_SIZE_BYTES: int = 31457280

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""

XROOTD_OPEN_TIMEOUT_SEC: int = 10
XROOTD_READ_TIMEOUT_SEC: int = 120
HTTP_GET_TIMEOUT_SEC: int = 120

FETCH_MAX_WORKERS: int = 4

LOGS = []


def handle(req: bytes) -> str:
    """
    Downloads files to be fetched and puts them under given path

    Args Structure:
        heartbeatUrl (str): url where heartbeats are posted to, automatically added to lambda
        filesToFetch (batch/list of objects): informations about files to be fetched,
            using format:{"url": <str>, "size": <int>, "path": <str>}

    Return:
        uploadedFiles (batch/list of strings): list of file paths, which were successfully fetched and
            placed under given path.
        logs (batch/list of strings): list of objects describing log from lambda
    """

    global LOGS
    global HEARTBEAT_URL

    data = json.loads(req)

    HEARTBEAT_URL = data["ctx"]["heartbeatUrl"]
    heartbeat()

    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as executor:
        results = list(executor.map(process_item, data["argsBatch"]))
    return json.dumps({"resultsBatch": results})


def process_item(args):
    try:
        file_info = args["fileToFetch"]
        url = file_info["url"]
        size = file_info["size"]
        path = f'/mnt/onedata/{file_info["path"]}'

        try_download_file(url, size, path)
        error_logs = []
        for log_object in LOGS:
            if log_object["severity"] == "error":
                error_logs.append(log_object)
        if len(error_logs) >= 1:
            return {
                "exception": {
                    "reason": error_logs
                }
            }

        return {
            "uploadedFile": path,
            "logs": LOGS
        }
    except Exception as ex:
        return {
            "exception": {
                "reason": [str(ex)]
            }
        }


def try_download_file(file_url: str, file_size: int, file_path: str):
    global LOGS
    download_logs = []
    try:
        for backoff_sec in [0, 2, 5, 7]:
            try:
                # clear logs from previous retries
                download_logs = []
                if file_exists(file_path, file_size):
                    download_logs.append({
                        "severity": "info",
                        "file": file_path,
                        "status": f"File with expected size {file_size} Bytes already exists.",
                        "envs": dict(os.environ)
                    }),
                    break
                download_file(file_url, file_size, file_path),
                break
            except Exception as ex:
                time.sleep(backoff_sec)
                if backoff_sec == 7:
                    raise ex
        LOGS.extend(download_logs)

    except Exception as e:
        LOGS.extend(download_logs)
        LOGS.append({
            "severity": "error",
            "file": file_path,
            "status": str(e),
            "envs": dict(os.environ)
        })


def download_file(file_url: str, file_size: int, file_path: str):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    except Exception as ex:
        raise Exception(f"Failed to create dirs on path due to: {str(ex)}")

    if is_xrootd(file_url):
        download_xrootd_file(file_url, file_size, file_path)
    else:
        download_http_file(file_url, file_size, file_path)
    return


def is_xrootd(url: str) -> bool:
    return url.startswith("root:/")


def download_xrootd_file(file_url: str, file_size: int, file_path: str):
    heartbeat()
    with client.File() as source, open(file_path, 'wb') as destination:
        try:
            source.open(file_url, timeout=XROOTD_OPEN_TIMEOUT_SEC)
        except Exception as ex:
            raise Exception(f"Failed to open file under url: {file_url}. Reason: {str(ex)}")
        offset = 0
        while True:
            heartbeat()
            try:
                (_, data) = source.read(offset=offset, size=BLOCK_SIZE_BYTES, timeout=XROOTD_READ_TIMEOUT_SEC)
            except Exception as ex:
                raise Exception(f"Failed to read data from file under url: {file_url}. "
                                f"Url may be incorrect. Reason: {str(ex)}")
            if not data:
                break
            try:
                destination.write(data)
            except Exception as ex:
                raise Exception(f"Failed to write data to destination file: {file_path}. Reason: {str(ex)}")
            offset = offset + BLOCK_SIZE_BYTES
    assert_proper_file_size(file_path, file_size)


def download_http_file(file_url: str, file_size: int, file_path: str):
    heartbeat()
    try:
        r = requests.get(file_url, stream=True, allow_redirects=True, timeout=HTTP_GET_TIMEOUT_SEC)
    except Exception as ex:
        raise Exception(f"Failed to fetch file from url: {file_url}, due to: {str(ex)}")
    if not r.ok:
        raise Exception(f"HTTP/S file address: {file_url} is unreachable. Code: {str(r.status_code)}")
    try:
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(BLOCK_SIZE_BYTES):
                f.write(chunk)
                heartbeat()
    except Exception as e:
        raise Exception(f"Failed to write data to file due to: {str(e)}")
    assert_proper_file_size(file_path, file_size)


def assert_proper_file_size(file_path: str, file_size: int):
    time.sleep(3)
    downloaded_file_size = Path(file_path).stat().st_size
    if downloaded_file_size != file_size:
        raise Exception(f"Downloaded file has wrong size (expected size has been taken from fetch.txt file). "
                        f"Expected: {file_size} B, Downloaded: {downloaded_file_size} B")


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


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
