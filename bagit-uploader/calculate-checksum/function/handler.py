import hashlib
import json
import os.path
from typing import IO
import time
import xattr
import zlib

import requests

SUPPORTED_CHECKSUM_ALGORITHMS: tuple = ('md5', 'sha1', 'sha256', 'sha512', 'adler32')
BLOCK_SIZE: int = 262144

HEARTBEAT_INTERVAL_SEC: int = 150
LAST_HEARTBEAT_TIME: int = 0
HEARTBEAT_URL: str = ""


def handle(req: bytes) -> str:
    """Calculates checksums for file, and compares them with checksums from manifests, which were set as custom metadata
        under 'checksum.<algorithm>.expected' key previously.
    Args Structure:
        filePath (str): file path to proceed

    Returns:
        checksums (object): information about checksums correctness
            using format: {"filePath": (str), <algorithm>: "ok"/{"calculated": (str), "expected": (str)}}
    """
    global HEARTBEAT_URL

    args = json.loads(req)

    HEARTBEAT_URL = args["heartbeatUrl"]
    heartbeat()

    file_path = args["filePath"]
    file_info = {}
    try:
        if os.path.isfile(file_path):
            for algorithm in SUPPORTED_CHECKSUM_ALGORITHMS:
                xf = xattr.xattr(file_path)
                expected_checksum_key = f'checksum.{algorithm}.expected'
                calculated_checksum_key = f'checksum.{algorithm}.calculated'
                if expected_checksum_key in xf.list():
                    try:
                        with open(file_path, 'rb') as fd:
                            calculated_checksum = calculate_checksum(fd, algorithm)
                    except Exception as ex:
                        raise Exception(f"Failed to open file and calculate checksum due to: {str(ex)}")

                    try:
                        exp_checksum = xf.get(expected_checksum_key).decode("utf8")
                        xf.set(calculated_checksum_key, str.encode(f"\"{calculated_checksum}\""))
                    except Exception as ex:
                        raise Exception(f"Failed set calculated checksum metadata due to: {str(ex)}")

                    file_info["file"] = file_path
                    if exp_checksum != calculated_checksum:
                        raise Exception(
                            f"Expected file checksum: {exp_checksum}, when calculated checksum is: {calculated_checksum}")

                    file_info[algorithm] = {"expected": exp_checksum, "calculated": calculated_checksum, "status": "ok"}
    except Exception as ex:
        return json.dumps({"exception": {
            "reason": f"Checksum verification failed due to: {str(ex)}"
        }})
    return json.dumps({"checksums": file_info})


def calculate_checksum(fd: IO[bytes], algorithm: str) -> str:
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
    if current_time - LAST_HEARTBEAT_TIME > HEARTBEAT_INTERVAL_SEC:
        r = requests.post(url=HEARTBEAT_URL, data={})
        if r.ok:
            LAST_HEARTBEAT_TIME = current_time
