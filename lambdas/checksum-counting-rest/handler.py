"""
A lambda which calculates (and saves as metadata) file checksum using REST interface.

NOTE: This lambda works on any type of file by simply returning `None` 
as checksum for anything but regular files.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import hashlib
import json
import multiprocessing as mp
import os
import threading
import time
import traceback
import zlib
from typing import Final, Iterator, NamedTuple, Optional, Set, Union

import requests
from openfaas_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
)
from typing_extensions import TypedDict


AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"adler32"}, hashlib.algorithms_available
)
DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2
VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") == "true"


class AtmJobArgs(TypedDict):
    file: AtmFile
    algorithm: str
    metadataKey: str


class AtmFileChecksumReport(TypedDict):
    file_id: str
    algorithm: str
    checksum: Optional[str]


class AtmJobResults(TypedDict):
    result: AtmFileChecksumReport


class AtmJob(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: AtmJobArgs


def handle(
    job_batch_request: AtmJobBatchRequest[AtmJobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[AtmJobResults]:

    jobs_monitor = threading.Thread(target=monitor_jobs, args=[heartbeat_callback])
    jobs_monitor.daemon = True
    jobs_monitor.start()

    jobs = [
        AtmJob(args=job_args, ctx=job_batch_request["ctx"])
        for job_args in job_batch_request["argsBatch"]
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        job_results = list(executor.map(run_job, jobs))

    return {"resultsBatch": job_results}


def run_job(job: AtmJob) -> Union[AtmException, AtmJobResults]:
    if job.args["file"]["type"] != "REG":
        return build_job_results(job, None)

    if job.args["algorithm"] not in AVAILABLE_CHECKSUM_ALGORITHMS:
        return AtmException(
            exception=(
                f"{job.args['algorithm']} algorithm is unsupported."
                f"Available ones are: {AVAILABLE_CHECKSUM_ALGORITHMS}"
            )
        )

    try:
        data_stream = get_file_data_stream(job)
        checksum = calculate_checksum(job, data_stream)

        if job.args["metadataKey"] != "":
            set_file_metadata(job, checksum)
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job, checksum)


def build_job_results(job: AtmJob, checksum: Optional[str]) -> AtmJobResults:
    return {
        "result": {
            "file_id": job.args["file"]["file_id"],
            "algorithm": job.args["algorithm"],
            "checksum": checksum,
        }
    }


def get_file_data_stream(job: AtmJob) -> Iterator[bytes]:
    response = requests.get(
        build_file_rest_url(job, "content"),
        headers={"x-auth-token": job.ctx["accessToken"]},
        stream=True,
        verify=VERIFY_SSL_CERTS,
    )
    response.raise_for_status()

    return response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE)


def calculate_checksum(job: AtmJob, data_stream: Iterator[bytes]) -> str:
    algorithm = job.args["algorithm"]

    if algorithm == "adler32":
        value = 1
        for data in data_stream:
            _downloaded_block_sizes.put(len(data))
            value = zlib.adler32(data, value)
        return format(value, "x")
    else:
        hash = getattr(hashlib, algorithm)()
        for data in data_stream:
            _downloaded_block_sizes.put(len(data))
            hash.update(data)
        return hash.hexdigest()


def set_file_metadata(job: AtmJob, checksum: str) -> None:
    response = requests.put(
        build_file_rest_url(job, "metadata/xattrs"),
        headers={
            "x-auth-token": job.ctx["accessToken"],
            "content-type": "application/json",
        },
        json={job.args["metadataKey"]: checksum},
        verify=VERIFY_SSL_CERTS,
    )
    response.raise_for_status()


def build_file_rest_url(job: AtmJob, subpath: str) -> str:
    return "https://{domain}/api/v3/oneprovider/data/{file_id}/{subpath}".format(
        domain=job.ctx["oneproviderDomain"],
        file_id=job.args["file"]["file_id"],
        subpath=subpath.lstrip("/"),
    )


_downloaded_block_sizes: mp.Queue = mp.Queue()


def monitor_jobs(heartbeat_callback: AtmHeartbeatCallback) -> None:
    while True:
        time.sleep(1)

        throughput = 0
        while not _downloaded_block_sizes.empty():
            throughput += _downloaded_block_sizes.get()

        if throughput != 0:
            heartbeat_callback()

            measurement = {
                "tsName": "throughput",
                "value": throughput,
                "timestamp": int(time.time()),
            }
            with open("/out/stats", "a") as f:
                json.dump(measurement, f)
                f.write("\n")
