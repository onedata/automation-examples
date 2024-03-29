"""A lambda which archives destination directory."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import json
import os
import os.path
import queue
import time
import traceback
from threading import Event, Thread
from typing import Dict, Final, NamedTuple, Union

import requests
from typing_extensions import TypedDict

from onedata_lambda_utils.logging import AtmLogger
from onedata_lambda_utils.stats import AtmTimeSeriesMeasurementBuilder
from onedata_lambda_utils.streaming import AtmResultStreamer
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmObject,
    AtmTimeSeriesMeasurement,
)

##===================================================================
## Lambda configuration
##===================================================================


VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"
ARCHIVE_STATUS_CHECK_INTERVAL_SEC: Final[int] = 5
REST_REQUEST_TIMEOUT: Final[int] = 60


##===================================================================
## Lambda interface
##===================================================================


LOGS_STREAMER: Final[AtmLogger[AtmObject]] = AtmLogger(
    result_name="logs", synchronized=True
)

STATS_STREAMER: Final[AtmResultStreamer[AtmTimeSeriesMeasurement]] = AtmResultStreamer(
    result_name="stats", synchronized=False
)


class FilesArchived(
    AtmTimeSeriesMeasurementBuilder, ts_name="filesArchived", unit=None
):
    pass


class BytesArchived(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesArchived", unit="Bytes"
):
    pass


class JobArgs(TypedDict):
    destinationDir: AtmFile


class JobResults(TypedDict):
    archiveId: str


##===================================================================
## Lambda implementation
##===================================================================


class JobException(Exception):
    exception: str


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:
    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    jobs = [
        Job(args=job_args, ctx=job_batch_request["ctx"])
        for job_args in job_batch_request["argsBatch"]
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(run_job, jobs))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": results}


def run_job(job: Job) -> Union[JobResults, AtmException]:
    try:
        dataset_id = establish_dataset(job)
        archive_id = create_archive(job, dataset_id)
        await_archive_preserved(job, archive_id)
    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"archiveId": archive_id}


def establish_dataset(job: Job) -> str:
    resp = requests.post(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/datasets',
        headers={
            "x-auth-token": job.ctx["accessToken"],
            "content-type": "application/json",
        },
        data=json.dumps(
            {"rootFileId": job.args["destinationDir"]["fileId"], "protectionFlags": []}
        ),
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )

    if resp.status_code == 201:
        result = resp.json()["datasetId"]

    elif resp.status_code == 409:
        LOGS_STREAMER.warning(
            {
                "destinationDir": job.args["destinationDir"]["fileId"],
                "message": "Dataset already established.",
            }
        )
        result = get_dst_dir_dataset_id(job)

    else:
        resp.raise_for_status()

    return result


def get_dst_dir_dataset_id(job: Job) -> str:
    host = job.ctx["oneproviderDomain"]
    dst_dir_id = job.args["destinationDir"]["fileId"]

    resp = requests.get(
        f"https://{host}/api/v3/oneprovider/data/{dst_dir_id}/dataset/summary",
        headers={"x-auth-token": job.ctx["accessToken"]},
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )
    resp.raise_for_status()

    return resp.json()["directDataset"]


def create_archive(job: Job, dataset_id: str) -> str:
    resp = requests.post(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/archives',
        headers={
            "x-auth-token": job.ctx["accessToken"],
            "content-type": "application/json",
        },
        data=json.dumps(
            {
                "datasetId": dataset_id,
                "config": {
                    "includeDip": True,
                    "layout": "bagit",
                },
            }
        ),
        verify=VERIFY_SSL_CERTS,
        timeout=REST_REQUEST_TIMEOUT,
    )
    resp.raise_for_status()

    return resp.json()["archiveId"]


def await_archive_preserved(job: Job, archive_id: str) -> None:
    bytes_archived = 0
    files_archived = 0

    while True:
        archive_info = get_archive_info(job, archive_id)

        archive_stats = archive_info["stats"]
        if bytes_diff := archive_stats["bytesArchived"] - bytes_archived:
            _measurements_queue.put(BytesArchived.build(bytes_diff))
            bytes_archived += bytes_diff
        if files_diff := archive_stats["filesArchived"] - files_archived:
            _measurements_queue.put(FilesArchived.build(files_diff))
            files_archived += files_diff

        if archive_info["state"] == "preserved":
            break

        if archive_info["state"] in ("pending", "building", "verifying"):
            time.sleep(ARCHIVE_STATUS_CHECK_INTERVAL_SEC)

        else:
            raise JobException(
                f'Archivisation (id: "{archive_id}") failed with status: {archive_info["status"]}'
            )


def get_archive_info(job: Job, archive_id: str) -> Dict:
    resp = requests.get(
        f'https://{job.ctx["oneproviderDomain"]}/api/v3/oneprovider/archives/{archive_id}',
        headers={"x-auth-token": job.ctx["accessToken"]},
        verify=VERIFY_SSL_CERTS,
        allow_redirects=True,
        timeout=REST_REQUEST_TIMEOUT,
    )
    resp.raise_for_status()

    return resp.json()


def monitor_jobs(heartbeat_callback: AtmHeartbeatCallback) -> None:
    any_job_ongoing = True
    while any_job_ongoing:
        any_job_ongoing = not _all_jobs_processed.wait(timeout=1)

        measurements = []
        while not _measurements_queue.empty():
            measurements.append(_measurements_queue.get())

        if measurements:
            STATS_STREAMER.stream_items(measurements)
            heartbeat_callback()
