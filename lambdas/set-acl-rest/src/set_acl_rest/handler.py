"""
A lambda which sets ACL metadata on a file using Oneprovider REST API.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
from typing import Final, TypedDict

import requests
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    per_job,
)


##===================================================================
## Lambda configuration
##===================================================================


REST_REQUEST_TIMEOUT: Final[int] = 60
ENV_VERIFY_SSL: Final[str] = "VERIFY_SSL_CERTIFICATES"


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    targetFile: AtmFile
    acl: list[AtmObject]


type JobResult = None


##===================================================================
## Lambda implementation
##===================================================================


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    try:
        _set_acl(job.args, ctx)
    except requests.RequestException as ex:
        raise JobException(f"REST request failed: {ex}") from ex


def _set_acl(job_args: JobArgs, ctx: JobContext[AtmObject]) -> None:
    response = requests.put(
        _build_set_acl_rest_url(ctx.oneprovider_domain, job_args["targetFile"]["fileId"]),
        headers={
            "x-auth-token": ctx.access_token,
            "content-type": "application/json",
        },
        json={"cdmi_acl": job_args["acl"]},
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )
    response.raise_for_status()


def _build_set_acl_rest_url(domain: str, file_id: str) -> str:
    return f"https://{domain}/api/v3/oneprovider/data/{file_id}/metadata/xattrs"


def _verify_ssl() -> bool:
    return os.environ.get(ENV_VERIFY_SSL) != "false"
