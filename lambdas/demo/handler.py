"""
A simple lambda demonstrating basic lambda layout and utilities.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import traceback
from typing import Union

from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)
from typing_extensions import TypedDict

##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    item: AtmFile


class JobResults(TypedDict):
    result: str


##===================================================================
## Lambda implementation
##===================================================================


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    results = []
    for job_args in job_batch_request["argsBatch"]:
        results.append(run_job(job_args))
        heartbeat_callback()

    return {"resultsBatch": results}


def run_job(job_args: JobArgs) -> Union[AtmException, JobResults]:
    try:
        return {"result": f'Hello - {job_args["item"]["name"]}'}
    except Exception:
        return AtmException(exception=traceback.format_exc())
