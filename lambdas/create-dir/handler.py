"""
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os

import requests
import concurrent.futures
import traceback
from threading import Event
from typing_extensions import TypedDict, NamedTuple
from typing import Final, Union


from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmJobBatchRequestCtx,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject
)

##===================================================================
## Lambda configuration
##===================================================================


VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"
REST_REQUEST_TIMEOUT: Final[int] = 60


##===================================================================
## Lambda interface
##===================================================================


class TaskConfig(TypedDict):
    sleepDurationSec: float
    exceptionProbability: float  # range: [0, 1]
    streamResults: bool


class JobArgs(TypedDict):
    dirName: str
    parentDir: AtmFile


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


_all_jobs_processed: Event = Event()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
) -> AtmJobBatchResponse[JobResults]:
    jobs = [
        Job(args=job_args, ctx=job_batch_request["ctx"])
        for job_args in job_batch_request["argsBatch"]
    ]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(run_job, jobs))

    _all_jobs_processed.set()

    return {"resultsBatch": results}


def run_job(job: Job) -> Union[JobResults, AtmException]:
    try:
        file_id = create_directory(job)
        file2_id = create_item_in_directory(job, "input")
        file3_id = create_item_in_directory(job, "results")
        file4_id = create_item_in_directory(job, "meta")
    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"fileId": file_id}


def create_item_in_directory(job: Job, name: str) -> str:
    resp = requests.post(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/data/'
        f'{job.args["parentId"]}/children?name={name}&type=DIR',
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


def create_directory(job: Job) -> str:
    resp = requests.post(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/data/'
        f'{job.args["parentId"]}/path/{job.args["name"]}?type=DIR',
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
