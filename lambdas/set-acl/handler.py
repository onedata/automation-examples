"""
A lambda which sets alc to the given file
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import json
import os
import traceback
from typing import Final, NoReturn, Union

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
    targetFileId: AtmFile
    acl: str


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
) -> None:

    jobs = [
        Job(ctx=job_batch_request["ctx"], args=job_args)
        for job_args in job_batch_request["argsBatch"]
    ]

    for job in jobs:
        run_job(job)
        heartbeat_callback()


def run_job(job: Job) -> Union[NoReturn, AtmException]:
    try:
        set_acl(job)
    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())


def set_acl(job: Job) -> NoReturn:
    resp = requests.put(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/data/{job.args["targetFileId"]["fileId"]}/'
        f"metadata/xattrs",
        headers={
            "x-auth-token": job.ctx["accessToken"],
            "content-type": "application/json",
        },
        data=json.dumps({"cdmi_acl": job.args["acl"]}),
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )

    if resp.status_code == 204:
        return
    if resp.status_code == 404:
        raise Exception("file not found")
    raise resp.raise_for_status()
