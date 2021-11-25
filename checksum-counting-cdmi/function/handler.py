import hashlib
import zlib
import json
import requests
import urllib3

SUPPORTED_CHECKSUM_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'adler32']
BLOCK_SIZE = 262144

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    args = json.loads(req)

    results = [process_item(item) for item in args["argsBatch"]]
    return json.dumps({"resultsBatch": results})


def process_item(args):
    file_id = args["item"]["file_id"]
    host = args["credentials"]["host"]
    access_token = args["credentials"]["accessToken"]
    algorithm = args["algorithm"]
    metadata_key = args["metadata_key"]

    response = get_file_request_response(access_token, host, file_id)
    checksum = calculate_checksum(response, algorithm)

    if metadata_key != "":
        set_metadata(file_id, metadata_key, checksum, access_token, host)

    return {"result": {
        "file_id": file_id,
        "checksum": checksum,
        "algorithm": algorithm
    }}


def get_file_request_response(access_token, host, file_id):
    url = f'https://{host}/cdmi/cdmi_objectid/{file_id}'
    headers = {"X-Auth-Token": access_token}
    response = requests.get(url, headers=headers, verify=False)
    return response


def calculate_checksum(response, algorithm):
    if algorithm == "adler32":
        checksum = 1
        for data in response.iter_content(chunk_size=BLOCK_SIZE):
            checksum = zlib.adler32(data, checksum)
        return format(checksum, 'x')
    else:
        checksum = getattr(hashlib, algorithm)()
        for data in response.iter_content(chunk_size=BLOCK_SIZE):
            checksum.update(data)
        return checksum.hexdigest()


def set_metadata(file_id, metadata_key, checksum, access_token, host):
    url = f'https://{host}/api/v3/oneprovider/data/{file_id}/metadata/xattrs'
    headers = {"X-Auth-Token": access_token, "Content-Type": "application/json"}
    payload = {metadata_key: checksum}
    requests.put(url, headers=headers, data=json.dumps(payload), verify=False)
