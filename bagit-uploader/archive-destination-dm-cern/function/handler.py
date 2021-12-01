import json
import time
import os

import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    global HEARTBEAT_URL

    data = json.loads(req)

    HEARTBEAT_URL = data["ctx"]["heartbeatUrl"]
    heartbeat()

    results = [process_item(item) for item in data["argsBatch"]]
    return json.dumps({"resultsBatch": results})


def process_item(args):
    logs = []

    try:
        destination = args["destination"]
        destination_id = destination["file_id"]
        destination_name = destination["name"]
        archive_name, _ = os.path.splitext(args["archive"]["name"])

        accessToken = args["credentials"]["accessToken"]
        host = args["credentials"]["host"]
        headers = {"X-Auth-Token": accessToken, "Content-Type": "application/json"}

        dataset_id = ensure_dataset_established(host, destination_id, headers, destination_name, logs)

        archive_id = create_archive(headers, host, dataset_id, archive_name)
        wait_until_archive_is_ready(host, headers, archive_id)

        logs.append({
            "severity": "info",
            "DatasetId": dataset_id,
            "ArchiveId": archive_id
        })

    except Exception as ex:
        return {
            "exception": {
                "status": f" Creating archive failed due to: {str(ex)}"
            }
        }

    return {
        "response": {
            "datasetId": dataset_id,
            "archiveId": archive_id,
        },
        "logs": logs
    }


def ensure_dataset_established(host: str, destination_id: str, headers: dict, destination_name: str, logs: list) -> str:
    try:
        url = f'https://{host}/api/v3/oneprovider/datasets'
        data = {
            "rootFileId": destination_id,
            "protectionFlags": []
        }
        resp = requests.post(url, headers=headers, data=json.dumps(data), verify=False)
        assert resp.ok
        dataset_id = resp.json()["datasetId"]
        return dataset_id
    except:
        # dataset has been establish previously, try to get dataset_id established on destination directory
        url = f'https://{host}/api/v3/oneprovider/data/{destination_id}/dataset/summary'
        resp = requests.get(url, headers=headers, verify=False)
        assert resp.ok
        dataset_id = resp.json()["directDataset"]

        logs.append({
            "severity": "info",
            "status": f"Dataset on directory {destination_name} already established with datasetId: {dataset_id} "
        })
        return dataset_id


def create_archive(headers: dict, host: str, dataset_id: str, archive_name: str) -> str:
    url = f'https://{host}/api/v3/oneprovider/archives'
    data = {
        "datasetId": dataset_id,
        "config": {
            "incremental": {
                "enabled": True},
            "includeDip": True,
            "layout": "bagit"
        },
        "description": f'_USE_FILENAME:{archive_name}'
    }
    resp = requests.post(url, headers=headers, data=json.dumps(data), verify=False)
    if not resp.ok:
        raise Exception(
            f"Failed to create archive via REST. Code: {resp.status_code}, RequestBody: {json.dumps(data)}")
    archive_id = resp.json()["archiveId"]
    return archive_id


def wait_until_archive_is_ready(host: str, headers: dict, archive_id: str):
    latest_bytes_preserved = 0
    current_bytes_preserved = 0
    latest_check_time = 0
    increment_timeout_sec = 60
    while True:
        try:
            url = f'https://{host}/api/v3/oneprovider/archives/{archive_id}'
            resp = requests.get(url, headers=headers, verify=False, allow_redirects=True)

            if not resp.ok:
                raise Exception(f"Failed to get archive details via REST. Code: {resp.status_code}")
            latest_check_time = int(time.time())

            archive_state = resp.json()["state"]
            current_bytes_preserved = resp.json()["stats"]["bytesArchived"]
            if archive_state == "preserved":
                return
            else:
                raise Exception(
                    f"Archive is not preserved yet. Archive status: {archive_state}. Preserved {current_bytes_preserved} Bytes")

        except Exception as ex:
            current_time = int(time.time())
            bytes_progress = int(current_bytes_preserved) - int(latest_bytes_preserved)
            time_diff = current_time - latest_check_time
            if time_diff >= increment_timeout_sec and bytes_progress == 0:
                purge_status = purge_archive(host, headers, archive_id)
                raise Exception(
                    f"Could not create archive. No progress has been made within latest 60 sec. Reason: {str(ex)}. "
                    f"Purged archive with result: {purge_status}")
            elif bytes_progress >= 0:
                latest_check_time = current_time

            latest_bytes_preserved = current_bytes_preserved
            heartbeat()
            time.sleep(10)


def purge_archive(host: str, headers: dict, archive_id: str) -> str:
    try:
        url = f'https://{host}/api/v3/oneprovider/archives/{archive_id}/init_purge'
        resp = requests.post(url, headers=headers, verify=False, data=json.dumps({}), allow_redirects=True, timeout=10)
        if not resp.ok:
            raise Exception(f"Failed to purge archive via REST. Code: {resp.status_code}")
        return f"Successfully purged archive."
    except Exception as ex:
        return f"Failed to purge archive due to: {str(ex)}"


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
