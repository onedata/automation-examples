"""A simple lambda demonstrating the basic lambda layout: it greets each input file by name."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import TypedDict

from onedata_lambda_sdk import AtmFile, AtmObject, Job, JobContext, per_job


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    item: AtmFile


class JobResult(TypedDict):
    result: str


##===================================================================
## Lambda implementation
##===================================================================


@per_job
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    return {"result": f"Hello - {job.args['item']['name']}"}
