"""
A lambda which creates a directory structure, expressed using a list of paths,
in the target directory. It will ensure that all provided paths exist
and each path element is a directory, or fail otherwise.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os
from http import HTTPStatus
from pathlib import PurePosixPath
from typing import Any, Final, TypedDict
from urllib.parse import quote

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


# ===================================================================
## Lambda configuration
##===================================================================


REST_REQUEST_TIMEOUT: Final[int] = 60
ENV_VERIFY_SSL: Final[str] = "VERIFY_SSL_CERTIFICATES"


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    targetDir: AtmFile
    dirPaths: list[str]


class JobResult(TypedDict):
    directories: list[AtmFile]


##===================================================================
## Lambda implementation
##===================================================================


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    parent_id = job.args["targetDir"]["fileId"]
    parent_path = job.args["targetDir"]["path"]

    dir_objects: list[AtmFile] = [
        {"fileId": _create_dir(parent_id, parent_path, dir_path, ctx)}
        for dir_path in job.args["dirPaths"]
    ]

    return {"directories": dir_objects}


def _create_dir(parent_id: str, parent_path: str, path: str, ctx: JobContext[AtmObject]) -> str:
    resp = requests.put(
        _build_create_dir_rest_url(ctx.oneprovider_domain, parent_id, path),
        params={"type": "DIR", "create_parents": "true"},
        headers={
            "x-auth-token": ctx.access_token,
        },
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )

    if resp.status_code == HTTPStatus.CREATED:
        return _extract_file_id(resp.json())
    if resp.status_code == HTTPStatus.BAD_REQUEST:
        reason = resp.json()["error"]["details"]["errno"]
        if reason == "eexist":
            return _get_file_id(parent_path, path, ctx)
        if reason == "enotdir":
            raise JobException(f'"{path}" path already exists and is not a directory')
    resp.raise_for_status()
    raise JobException(f"Unexpected no error response status code {resp.json()}")


def _build_create_dir_rest_url(domain: str, parent_id: str, path: str) -> str:
    return f"https://{domain}/api/v3/oneprovider/data/{parent_id}/path/{_encode_url_path(path)}"


def _get_file_id(parent_path: str, path: str, ctx: JobContext[AtmObject]) -> str:
    resp = requests.post(
        _build_get_file_id_rest_url(ctx.oneprovider_domain, parent_path, path),
        headers={
            "x-auth-token": ctx.access_token,
        },
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )

    resp.raise_for_status()
    return _extract_file_id(resp.json())


def _extract_file_id(payload: dict[str, Any]) -> str:
    file_id = payload["fileId"]
    if not isinstance(file_id, str):
        raise JobException(f"Invalid fileId in REST response: {payload}")
    return file_id


def _build_get_file_id_rest_url(domain: str, parent_path: str, path: str) -> str:
    absolute_path = PurePosixPath(parent_path) / path.lstrip("/")
    return (
        f"https://{domain}/api/v3/oneprovider/lookup-file-id/{_encode_url_path(str(absolute_path))}"
    )


def _encode_url_path(path: str) -> str:
    return quote(path, safe="/")


def _verify_ssl() -> bool:
    return os.environ.get(ENV_VERIFY_SSL) != "false"
