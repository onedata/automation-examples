import os
import json
import urllib3

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def handle(req: bytes):
    """Establishes dataset on given destination directory, and creates archive.

    Args Structure:
        heartbeatUrl (str): url where heartbeats are posted to, automatically added to lambda
        destination (dir-file): destination to create archive from"""
    args = json.loads(req)

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

    # get number of archives created from this dataset
    url3 = f'https://{host}/api/v3/oneprovider/datasets/{dataset_id}/archives'
    resp3 = requests.get(url3, headers=headers, verify=False)
    assert resp3.ok

    existing_archives = json.loads(resp3.text)["archives"]

    archive_name, archive_type = os.path.splitext(args["description"]["name"])

    if existing_archives:
        url8 = f'https://{host}/api/v3/oneprovider/archives/{existing_archives[0]}'
        resp8 = requests.get(url8, headers=headers, verify=False)
        assert resp8.ok
        # latest = json.loads(resp8.text)["creationTime"]

        latest = get_latest_archive(args, existing_archives)
        data4 = {
            "datasetId": dataset_id,
            "config": {
                "incremental": {
                    "enabled": True,
                    "basedOn": latest
                },
                "includeDip": True,
                "layout": "bagit"
            },
            "description": f'_USE_FILENAME:{archive_name}'
        }

    else:
        latest = "unknown"
        data4 = {
            "datasetId": dataset_id,
            "config": {
                "incremental": {"enabled": True},
                "includeDip": True,
                "layout": "bagit"
            },
            "description": f'_USE_FILENAME:{archive_name}'
        }

    url4 = f'https://{host}/api/v3/oneprovider/archives'
    resp4 = requests.post(url4, headers=headers, data=json.dumps(data4), verify=False)
    archive_id = json.loads(resp4.text)["archiveId"]

    # format object-like response
    return json.dumps({"response": {
        "datasetId": dataset_id,
        "archiveId": archive_id,
        "latest": latest
    }})


def get_latest_archive(args, archives):
    accessToken = args["credentials"]["accessToken"]
    host = args["credentials"]["host"]
    headers = {"X-Auth-Token": accessToken, "Content-Type": "application/json"}
    archives_timestamps = {}
    for archive in archives:
        url = f'https://{host}/api/v3/oneprovider/archives/{archive}'
        resp = requests.get(url, headers=headers, verify=False)
        assert resp.ok
        timestamp = json.loads(resp.text)["creationTime"]
        archives_timestamps[archive] = timestamp
    max_timestamp = max(archives_timestamps.values())
    for archive in archives_timestamps:
        if max_timestamp == archives_timestamps[archive]:
            return archive