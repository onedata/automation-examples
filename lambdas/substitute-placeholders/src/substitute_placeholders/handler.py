"""
A lambda that fills placeholders in a template based on provided mappings.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from string import Template
from typing import TypedDict

from onedata_lambda_utils import (
    AtmObject,
    Job,
    JobContext,
    JobException,
    per_job,
)


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    template: str
    mappings: dict[str, str]


class JobResult(TypedDict):
    output: str


##===================================================================
## Lambda implementation
##===================================================================


@per_job
def handle(job: Job[JobArgs], _ctx: JobContext[AtmObject]) -> JobResult:
    try:
        output = Template(job.args["template"]).substitute(job.args["mappings"])
    except KeyError as ex:
        raise JobException(f"Missing mapping for placeholder: {ex.args[0]}") from ex
    except ValueError as ex:
        raise JobException(f"Invalid template: {ex}") from ex

    return {"output": output}
