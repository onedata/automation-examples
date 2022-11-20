"""
A lambda which calculates (and saves as metadata) file checksum using OnedataFS.

NOTE: This lambda works on any type of file by simply returning `None` 
as checksum for anything but regular files.
"""

__author__ = "Rafał Widziszewski"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import hashlib
import os
import traceback
import zlib
from typing import Final, NamedTuple, Optional, Set, Union

from fs.onedatafs import OnedataFS
from openfaas_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
)
from typing_extensions import TypedDict

##===================================================================
## Lambda configuration
##===================================================================


AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"md5", "sha1", "sha256", "sha512", "adler32"}, hashlib.algorithms_available
)
DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2
VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    file: AtmFile
    algorithm: str
    metadataKey: str


class FileChecksumReport(TypedDict):
    file_id: str
    algorithm: str
    checksum: Optional[str]


class JobResults(TypedDict):
    result: FileChecksumReport


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    return {
        "resultsBatch": [
            run_job(job_batch_request, heartbeat_callback)
            for item in job_batch_request["argsBatch"]
        ]
    }


def run_job(job: Job, heartbeat_callback: AtmHeartbeatCallback) -> Union[AtmException, JobResults]:
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
        odfs = OnedataFS(
            job.ctx["oneproviderDomain"],
            job.ctx["accessToken"],
            force_proxy_io=True,
            insecure=True,
        )  # insecure not needed in case of "green" server certs

        with odfs.open(job.args["file"]["file_id"], "rb") as file:
            checksum = calculate_checksum(job, file, heartbeat_callback)

        odfs.setxattr(
            job.args["file"]["file_id"], "checksum", f'"{checksum}"'
        )  # xattr must be encoded as json
        odfs.close()

    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job, checksum)


def build_job_results(job: Job, checksum: Optional[str]) -> JobResults:
    return {
        "result": {
            "file_id": job.args["file"]["file_id"],
            "algorithm": job.args["algorithm"],
            "checksum": checksum,
        }
    }


def calculate_checksum(job: Job, file, heartbeat_callback: AtmHeartbeatCallback) -> str:
    algorithm = job.args["algorithm"]

    if algorithm == "adler32":
        value = 1
        while True:
            data = file.read(DOWNLOAD_CHUNK_SIZE)
            if not data:
                break
            value = zlib.adler32(data, value)
            heartbeat_callback()

        return format(checksum, "x")
    else:
        checksum = getattr(hashlib, algorithm)()
        while True:
            data = file.read(DOWNLOAD_CHUNK_SIZE)
            if not data:
                break
            checksum.update(data)
            heartbeat_callback()

        return checksum.hexdigest()
