"""
A lambda which fulfils given placeholders using provided mappings
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os
import traceback
from string import Template
from typing import Final, Mapping, Union

import requests
from typing_extensions import NamedTuple, TypedDict

from onedata_lambda_utils.types import (
    AtmException,
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
    template: str
    mappings: Mapping[str, str]


class JobResults(TypedDict):
    output: str


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
        template = Template(job.args["template"])
        output = template.substitute(job.args["mappings"])

    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"output": output}
