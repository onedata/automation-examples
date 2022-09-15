import concurrent.futures
import hashlib
import requests
import urllib3
import zlib


BLOCK_SIZE = 262144

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def handle(request):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        return {"resultsBatch": list(executor.map(process_item, request["argsBatch"]))}


def process_item(args):
    try:
        return process_item_insecure(args)
    except ex:
        return {"exception": {"reason": f"Calculating md5 failed due to: {str(ex)}"}}


def process_item_insecure(args):
    file_id = args["item"]["file_id"]
    credentials = args["credentials"]
    metadata_key = args["metadata_key"]

    data_stream = get_data_stream(file_id, credentials)
    md5_checksum = calculate_md5(data_stream)

    if metadata_key != "":
        set_metadata(file_id, metadata_key, md5_checksum, credentials)

    return {"result": {"file_id": file_id, "md5": md5_checksum}}


def get_data_stream(file_id, credentials):
    response = requests.get(
        f'https://{credentials["host"]}/api/v3/oneprovider/data/{file_id}/content', 
        headers={"X-Auth-Token": credentials["accessToken"]}, 
        stream=True, 
        verify=False
    )
    return response.iter_content(chunk_size=BLOCK_SIZE)


def calculate_md5(data_stream):
    md5_checksum = hashlib.md5()
    for data_chunk in data_stream:
        md5_checksum.update(data_chunk)

    return md5_checksum.hexdigest()


def set_metadata(file_id, metadata_key, md5_checksum, credentials):
    requests.put(
        f'https://{credentials["host"]}/api/v3/oneprovider/data/{file_id}/metadata/xattrs', 
        headers={"X-Auth-Token": credentials["accessToken"], "Content-Type": "application/json"},
        json={metadata_key: md5_checksum},
        verify=False
    )
