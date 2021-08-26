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

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()
    logs = []

    try:
        destination = args["destination"]
        destination_id = destination["file_id"]

        accessToken = args["credentials"]["accessToken"]
        host = args["credentials"]["host"]
        headers = {"X-Auth-Token": accessToken, "Content-Type": "application/json"}

        # try to establish dataset, may fail if dataset has been establish previously
        try:
            url1 = f'https://{host}/api/v3/oneprovider/datasets'
            data1 = {
                "rootFileId": destination_id,
                "protectionFlags": []
            }
            resp1 = requests.post(url1, headers=headers, data=json.dumps(data1), verify=False)
            assert resp1.ok
            dataset_id = json.loads(resp1.text)["datasetId"]
        except:
            # dataset has been establish previously, try to get dataset_id established on destination directory
            url2 = f'https://{host}/api/v3/oneprovider/data/{destination_id}/dataset/summary'
            resp2 = requests.get(url2, headers=headers, verify=False)
            assert resp2.ok
            dataset_id = json.loads(resp2.text)["directDataset"]
            destination_name = destination["name"]
            logs.append({
                "severity": "info",
                "status": f"Dataset on directory {destination_name} already established with datasetId: {dataset_id} "
            })

        # having dataset id, create archive from this dataset
        archive_name, archive_type = os.path.splitext(args["archive"]["name"])
        url3 = f'https://{host}/api/v3/oneprovider/archives'
        data3 = {
            "datasetId": dataset_id,
            "config": {
                "incremental": {
                    "enabled": True},
                "includeDip": True,
                "layout": "bagit"
            },
            "description": f'_USE_FILENAME:{archive_name}'
        }
        resp3 = requests.post(url3, headers=headers, data=json.dumps(data3), verify=False)
        if not resp3.ok:
            raise Exception(
                f"Failed to create archive via REST. Code: {resp3.status_code}, RequestBody: {json.dumps(data3)}")
        archive_id = json.loads(resp3.text)["archiveId"]

        wait_until_archive_is_ready(host, headers, archive_id)
        logs.append({
            "severity": "info",
            "DatasetId": dataset_id,
            "ArchiveId": archive_id
        })

    except Exception as ex:
        return json.dumps({
            "exception": {
                "status": f" Creating archive failed due to: {str(ex)}"
            }
        })

    # format object-like response
    return json.dumps({
        "response": {
            "datasetId": dataset_id,
            "archiveId": archive_id,
        },
        "logs": logs
    })


def wait_until_archive_is_ready(host, headers, archive_id):
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

            archive_state = json.loads(resp.text)["state"]
            current_bytes_preserved = json.loads(resp.text)["stats"]["bytesArchived"]
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
                raise Exception(
                    f"Could not create archive. No progress has been made within latest 60 sec. Reason: {str(ex)}")
            elif bytes_progress >= 0:
                latest_check_time = current_time

            latest_bytes_preserved = current_bytes_preserved
            heartbeat()
            time.sleep(10)


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
