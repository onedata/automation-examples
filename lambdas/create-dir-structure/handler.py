"""
A lambda which creates directory in given path, and subdirectories within it.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os

import requests
import traceback
from typing_extensions import TypedDict, NamedTuple
from typing import Final, Union, List


from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmJobBatchRequestCtx,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
    AtmHeartbeatCallback
)

##===================================================================
## Lambda configuration
##===================================================================


VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"
REST_REQUEST_TIMEOUT: Final[int] = 60


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    targetDir: AtmFile
    dirPaths: List[str]


class JobResults(TypedDict):
    fileIds: List[str]


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    exception: str


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs = [
        Job(ctx=job_batch_request["ctx"], args=job_args)
        for job_args in job_batch_request["argsBatch"]
    ]
    results = []
    for job in jobs:
        results.append(run_job(job))
        heartbeat_callback()

    return {"resultsBatch": results}


def run_job(job: Job) -> Union[JobResults, AtmException]:
    dirs_id = []
    try:
        dir_paths = job.args["dirPaths"]
        for dir_path in dir_paths:
            assert_valid_dir_path(dir_path)
            dirs_id.append(create_dir(job, dir_path))

    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"fileIds": dirs_id}


def assert_valid_dir_path(path: str):
    if '' in path.split("/"):
        raise Exception(f"wrong dir path: {path}")
    return


def build_create_dir_url(domain: str, parent_id: str, path: str) -> str:
    return f"https://{domain}/api/v3/oneprovider/data/{parent_id}/path/{path}"


def create_dir(job: Job, path: str) -> str:
    payload = {"type": "DIR", "create_parents": "true"}
    resp = requests.put(
        build_create_dir_url(job.ctx["oneproviderDomain"],
                             job.args["targetDir"]["file_id"], path),
        params=payload,
        headers={
            "x-auth-token": job.ctx["accessToken"],
        },
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )

    if resp.status_code == 201:
        return resp.json()["fileId"]
    elif resp.status_code == 400:
        reason = resp.json()["error"]["details"]["errno"]
        # If dir already exists
        if reason == "eexist":
            return " "
        # There is a file in given path
        elif reason == "enotdir":
            raise Exception(
                f"{path} path already exists and is not a directory")
        else:
            resp.raise_for_status()
    else:
        resp.raise_for_status()
