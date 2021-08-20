import json
import urllib3

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
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

    # having dataset id, create archive from this dataset
    url3 = f'https://{host}/api/v3/oneprovider/archives'
    data3 = {
        "datasetId": dataset_id,
        "config": {
            "incremental": {"enabled": True},
            "includeDip": True,
            "layout": "bagit"
        }
    }
    resp3 = requests.post(url3, headers=headers, data=json.dumps(data3), verify=False)
    archive_id = json.loads(resp3.text)["archiveId"]

    # format object-like response
    return json.dumps({"response": {
        "datasetId": dataset_id,
        "archiveId": archive_id
    }})
