import hashlib
import os.path
import zlib
import json
import xattr
import requests
import time

SUPPORTED_CHECKSUM_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'adler32']

BLOCK_SIZE = 262144

HEARTBEAT_URL = ""
LAST_HEARTBEAT_TIME = 0


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    global HEARTBEAT_URL

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    file_path = args["filePath"]
    file_info = {}

    if os.path.isfile(file_path):
        for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
            xf = xattr.xattr(file_path)
            expected_checksum_key = f'checksum.{algorithm}.expected'
            calculated_checksum_key = f'checksum.{algorithm}.calculated'
            if expected_checksum_key in xf.list():
                with open(file_path, 'rb') as fd:
                    calculated_checksum = calculate_checksum(fd, algorithm)
                exp_checksum = xf.get(expected_checksum_key).decode("utf8")
                xf.set(calculated_checksum_key, str.encode(calculated_checksum))
                file_info["file"] = file_path
                if exp_checksum != calculated_checksum:
                    file_info[algorithm] = {"expected": exp_checksum, "calculated": calculated_checksum}
                else:
                    file_info[algorithm] = "ok"
    return json.dumps({"brokenFile": file_info})


def calculate_checksum(fd, algorithm):
    if algorithm == "adler32":
        checksum = 1
        while True:
            heartbeat()
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum = zlib.adler32(data, checksum)
        return format(checksum, 'x')
    else:
        checksum = getattr(hashlib, algorithm)()
        while True:
            heartbeat()
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum.update(data)
        return checksum.hexdigest()


def heartbeat():
    global LAST_HEARTBEAT_TIME
    current_time = int(time.time())
    if current_time - LAST_HEARTBEAT_TIME > 150:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT_TIME = current_time
