"""
A lambda which creates a directory structure, expressed using a list of paths,
in the target directory. It will ensure that all provided paths exist
and each path element is a directory, or fail otherwise.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os
import traceback
from typing import Final, List, Union

import requests
from typing_extensions import NamedTuple, TypedDict

from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmObject,
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
    directories: List[AtmFile]


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

    results = []
    for job_args in job_batch_request["argsBatch"]:
        results.append(run_job(Job(ctx=job_batch_request["ctx"], args=job_args)))
        heartbeat_callback()

    return {"resultsBatch": results}


def run_job(job: Job) -> Union[JobResults, AtmException]:
    try:
        dir_ids = [create_dir(job, dir_path) for dir_path in job.args["dirPaths"]]

    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"directories": [{"fileId": file_id} for file_id in dir_ids]}


def create_dir(job: Job, path: str) -> str:
    resp = requests.put(
        build_create_dir_rest_url(job, path),
        params={"type": "DIR", "create_parents": "true"},
        headers={
            "x-auth-token": job.ctx["accessToken"],
        },
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )

    if resp.status_code == 201:
        return resp.json()["fileId"]
    if resp.status_code == 400:
        reason = resp.json()["error"]["details"]["errno"]
        if reason == "eexist":
            return get_file_id(job, path)
        if reason == "enotdir":
            raise JobException(f'"{path}" path already exists and is not a directory')
    resp.raise_for_status()
    raise JobException(f"Unexpected no error response status code {resp.json()}")


def build_create_dir_rest_url(job: Job, path: str) -> str:
    domain = job.ctx["oneproviderDomain"]
    parent_id = job.args["targetDir"]["fileId"]
    return f"https://{domain}/api/v3/oneprovider/data/{parent_id}/path/{path}"


def get_file_id(job: Job, path: str) -> str:
    resp = requests.post(
        build_get_file_id_rest_url(job, path),
        headers={
            "x-auth-token": job.ctx["accessToken"],
        },
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )

    resp.raise_for_status()
    return resp.json()["fileId"]


def build_get_file_id_rest_url(job: Job, path: str) -> str:
    domain = job.ctx["oneproviderDomain"]
    parent_path = job.args["targetDir"]["path"]
    absolute_path = parent_path + "/" + path
    return f"https://{domain}/api/v3/oneprovider/lookup-file-id/{absolute_path}"
