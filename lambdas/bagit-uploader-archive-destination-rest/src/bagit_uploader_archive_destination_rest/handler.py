"""A lambda which archives destination directory."""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import os
import time
from http import HTTPStatus
from typing import Any, Final, TypedDict, cast

import requests
from onedata_lambda_sdk import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    TimeSeriesMeasurementBuilder,
    per_job,
)


ARCHIVE_STATUS_CHECK_INTERVAL_SEC: Final[int] = 5
REST_REQUEST_TIMEOUT: Final[int] = 60
ENV_VERIFY_SSL: Final[str] = "VERIFY_SSL_CERTIFICATES"
STATS_STREAM: Final[str] = "stats"
LOGS_STREAM: Final[str] = "logs"


class FilesArchived(TimeSeriesMeasurementBuilder, ts_name="filesArchived", unit=None):
    pass


class BytesArchived(TimeSeriesMeasurementBuilder, ts_name="bytesArchived", unit="Bytes"):
    pass


class JobArgs(TypedDict):
    destinationDir: AtmFile


class JobResult(TypedDict):
    archiveId: str


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    try:
        dataset_id = _establish_dataset(job.args, ctx)
        archive_id = _create_archive(ctx, dataset_id)
        _await_archive_preserved(ctx, archive_id)
    except requests.RequestException as ex:
        raise JobException(f"REST request failed: {ex}") from ex

    return {"archiveId": archive_id}


def _establish_dataset(job_args: JobArgs, ctx: JobContext[AtmObject]) -> str:
    response = requests.post(
        _build_rest_url(ctx, "datasets"),
        headers={
            "x-auth-token": ctx.access_token,
            "content-type": "application/json",
        },
        json={"rootFileId": job_args["destinationDir"]["fileId"], "protectionFlags": []},
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )

    if response.status_code == HTTPStatus.CREATED:
        return cast(str, response.json()["datasetId"])

    if response.status_code == HTTPStatus.CONFLICT:
        ctx.logger(LOGS_STREAM).warning(
            {
                "destinationDir": job_args["destinationDir"]["fileId"],
                "message": "Dataset already established.",
            }
        )
        return _get_destination_dir_dataset_id(job_args, ctx)

    response.raise_for_status()
    raise JobException(f"Unexpected response while establishing dataset: {response.text}")


def _get_destination_dir_dataset_id(job_args: JobArgs, ctx: JobContext[AtmObject]) -> str:
    destination_dir_id = job_args["destinationDir"]["fileId"]
    response = requests.get(
        _build_rest_url(ctx, f"data/{destination_dir_id}/dataset/summary"),
        headers={"x-auth-token": ctx.access_token},
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return cast(str, response.json()["directDataset"])


def _create_archive(ctx: JobContext[AtmObject], dataset_id: str) -> str:
    response = requests.post(
        _build_rest_url(ctx, "archives"),
        headers={
            "x-auth-token": ctx.access_token,
            "content-type": "application/json",
        },
        json={
            "datasetId": dataset_id,
            "config": {
                "includeDip": True,
                "layout": "bagit",
            },
        },
        verify=_verify_ssl(),
        timeout=REST_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return cast(str, response.json()["archiveId"])


def _await_archive_preserved(ctx: JobContext[AtmObject], archive_id: str) -> None:
    bytes_archived = 0
    files_archived = 0
    stats = ctx.result_streamer(STATS_STREAM)

    while True:
        archive_info = _get_archive_info(ctx, archive_id)
        archive_stats = archive_info["stats"]

        if bytes_diff := archive_stats["bytesArchived"] - bytes_archived:
            stats.stream_item(BytesArchived.build(value=bytes_diff))
            bytes_archived += bytes_diff
        if files_diff := archive_stats["filesArchived"] - files_archived:
            stats.stream_item(FilesArchived.build(value=files_diff))
            files_archived += files_diff

        if archive_info["state"] == "preserved":
            return

        if archive_info["state"] in ("pending", "building", "verifying"):
            time.sleep(ARCHIVE_STATUS_CHECK_INTERVAL_SEC)
            continue

        raise JobException(
            f'Archivisation (id: "{archive_id}") failed with status: {archive_info["status"]}'
        )


def _get_archive_info(ctx: JobContext[AtmObject], archive_id: str) -> dict[str, Any]:
    response = requests.get(
        _build_rest_url(ctx, f"archives/{archive_id}"),
        headers={"x-auth-token": ctx.access_token},
        verify=_verify_ssl(),
        allow_redirects=True,
        timeout=REST_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return cast(dict[str, Any], response.json())


def _build_rest_url(ctx: JobContext[AtmObject], path: str) -> str:
    return f"https://{ctx.oneprovider_domain}/api/v3/oneprovider/{path.lstrip('/')}"


def _verify_ssl() -> bool:
    return os.environ.get(ENV_VERIFY_SSL) != "false"
