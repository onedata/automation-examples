import hashlib
import os.path
import zlib
import json
import xattr
import requests
import time

SUPPORTED_CHECKSUM_ALGORITHMS = ['md5', 'sha1', 'sha256', 'sha512', 'adler32']

BLOCK_SIZE = 262144


def handle(req: bytes):
    """handle a request to the function
    Args:
        req (str): request body
    """
    global HEARTBEAT_URL, LAST_HEARTBEAT

    args = json.loads(req)

    LAST_HEARTBEAT = time.time()
    HEARTBEAT_URL = args["heartbeatUrl"]

    file_path = args["filePath"]
    file_info = {}

    if os.path.isfile(file_path):
        with open(file_path, 'rb') as fd:
            for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
                xf = xattr.xattr(fd)
                exp_checksum_key = f'checksum.{algorithm}.expected'
                calculated_checksum_key = f'checksum.{algorithm}.calculated'
                if exp_checksum_key in xf.list():
                    exp_checksum = xf.get(exp_checksum_key).decode("utf8")
                    calculated_checksum = calculate_checksum(fd, algorithm)
                    xf.set(calculated_checksum_key, str.encode(calculated_checksum))
                    if exp_checksum != calculated_checksum:
                        file_info["file"] = file_path
                        file_info[algorithm] = {"expected": exp_checksum, "calculated": calculated_checksum}
    return json.dumps({"brokenFile": file_info})


def calculate_checksum(fd, algorithm):
    if algorithm == "adler32":
        checksum = 1
        while True:
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum = zlib.adler32(data, checksum)
        return format(checksum, 'x')
    else:
        checksum = getattr(hashlib, algorithm)()
        while True:
            data = fd.read(BLOCK_SIZE)
            if not data:
                break
            checksum.update(data)
        return checksum.hexdigest()


def heartbeat():
    global HEARTBEAT_URL, LAST_HEARTBEAT, HEARTBEAT_CYCLE
    if time.time() - LAST_HEARTBEAT > HEARTBEAT_CYCLE:
        r = requests.post(url=HEARTBEAT_URL, data={})
        assert r.ok
        LAST_HEARTBEAT = time.time()