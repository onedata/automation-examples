"""
A lambda which creates directory in given path, and 3 subdirectories within it.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os

import requests
import traceback
from typing_extensions import TypedDict, NamedTuple
from typing import Final, Union


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
    file: AtmFile
    name: str


class JobResults(TypedDict):
    fileId: str


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
    try:
        file_id = job.args["file"]["file_id"]
        file_name = job.args["name"]
        file_id = create_dir_in_parent(job, file_id, file_name)
        create_dir_in_parent(job, file_id, "input")
        create_dir_in_parent(job, file_id, "results")
        create_dir_in_parent(job, file_id, "meta")
    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"fileId": file_id}


def create_dir_in_parent(job: Job, parent_id: str, name: str) -> str:
    payload = {"name": name, "type": "DIR"}
    resp = requests.post(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/data/'
        f'{parent_id}/children',
        params=payload,
        headers={
            "x-auth-token": job.ctx["accessToken"],
        },
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )

    if resp.status_code == 201:
        return resp.json()["fileId"]
    else:
        resp.raise_for_status()
