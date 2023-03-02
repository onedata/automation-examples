"""A lambda which downloads files."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import dataclasses
import os
import os.path
import queue
import traceback
from threading import Event, Thread
from typing import Final, Union

import requests
from onedata_lambda_utils.stats import AtmTimeSeriesMeasurementBuilder
from onedata_lambda_utils.streaming import AtmResultStreamer
from onedata_lambda_utils.types import (
    AtmException,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import Annotated, TypedDict


##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"

HTTP_GET_TIMEOUT_SEC: Final[int] = 120
DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2


##===================================================================
## Lambda interface
##===================================================================


LOGS_STREAMER: Final[AtmResultStreamer[AtmObject]] = AtmResultStreamer(
    result_name="logs", synchronized=True
)

STATS_STREAMER: Final[AtmResultStreamer[AtmTimeSeriesMeasurement]] = AtmResultStreamer(
    result_name="stats", synchronized=False
)


class FilesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="filesProcessed", unit=None
):
    pass


class BytesProcessed(
    AtmTimeSeriesMeasurementBuilder, ts_name="bytesProcessed", unit="Bytes"
):
    pass


class FileDownloadInfo(TypedDict):
    sourceUrl: str
    destinationPath: str
    size: int


class JobArgs(TypedDict):
    downloadInfo: FileDownloadInfo


class JobResults(TypedDict):
    processedFilePath: str


##===================================================================
## Lambda implementation
##===================================================================


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(run_job, job_batch_request["argsBatch"]))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": results}


def run_job(job_args: JobArgs) -> Union[JobResults, AtmException]:
    try:
        run_job_insecure(job_args)
        _measurements_queue.put(FilesProcessed.build(value=1))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"processedFilePath": job_args["downloadInfo"]["destinationPath"]}


def build_result_destinationPath(job_args: JobArgs) -> str:
    if MOUNT_POINT in job_args["downloadInfo"]["destinationPath"]:
        job_args["downloadInfo"]["destinationPath"].replace(MOUNT_POINT, "")


def run_job_insecure(job_args: JobArgs) -> None:
    destination_path = build_destination_path(job_args)

    if os.path.exists(destination_path):
        if os.stat(destination_path).st_size == job_args["downloadInfo"]["size"]:
            LOGS_STREAMER.stream_item(
                {
                    "severity": "info",
                    "downloadInfo": job_args["downloadInfo"],
                    "message": (
                        "Skipping download as file with expected size "
                        "already exists at destination path."
                    ),
                }
            )
        else:
            LOGS_STREAMER.stream_item(
                {
                    "severity": "info",
                    "downloadInfo": job_args["downloadInfo"],
                    "message": (
                        "Removing file existing at destination path "
                        "(probably artefact of previous failed download).",
                    ),
                }
            )
            os.remove(destination_path)
            download_file(job_args)
    else:
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        download_file(job_args)


def download_file(job_args: JobArgs) -> None:
    r = requests.get(
        job_args["downloadInfo"]["sourceUrl"],
        stream=True,
        allow_redirects=True,
        timeout=HTTP_GET_TIMEOUT_SEC,
    )
    r.raise_for_status()

    file_size = 0
    with open(build_destination_path(job_args), "wb") as f:
        for chunk in r.iter_content(DOWNLOAD_CHUNK_SIZE):
            f.write(chunk)
            chunk_size = len(chunk)
            file_size += chunk_size
            _measurements_queue.put(BytesProcessed.build(value=chunk_size))

    if file_size != job_args["downloadInfo"]["size"]:
        raise Exception(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({file_size} B) size of downloaded file"
        )


def build_destination_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/{job_args["downloadInfo"]["destinationPath"]}'


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
