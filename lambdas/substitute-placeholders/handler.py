"""
A lambda that fills placeholders in a template based on provided mappings.
Uses the string python library.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import traceback
from string import Template
from typing import Mapping, Union

from typing_extensions import NamedTuple, TypedDict

from onedata_lambda_utils.types import (
    AtmException,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmObject,
)

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


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject]
) -> AtmJobBatchResponse[JobResults]:

    results = [run_job(job_args) for job_args in job_batch_request["argsBatch"]]
    return {"resultsBatch": results}


def run_job(job_args: JobArgs) -> Union[JobResults, AtmException]:
    try:
        template = Template(job_args["template"])
        output = template.substitute(job_args["mappings"])

    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"output": output}
