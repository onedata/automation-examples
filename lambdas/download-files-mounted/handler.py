"""A lambda which downloads files."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import dataclasses
import json
import os
import os.path
import queue
import traceback
from threading import Event, Lock, Thread
from typing import Final, List, Union

import requests
from openfaas_lambda_utils.stats import AtmStatsCounter, TSMetric
from openfaas_lambda_utils.types import (
    AtmException,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import Annotated as Ann
from typing_extensions import TypedDict


##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"

HTTP_GET_TIMEOUT_SEC: Final[int] = 120
DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2


##===================================================================
## Lambda interface
##===================================================================


LOGS_PIPED_RESULT_NAME: Final[str] = "logs"
STATS_PIPED_RESULT_NAME: Final[str] = "stats"


@dataclasses.dataclass
class StatsCounter(AtmStatsCounter):
    files_processed: Ann[int, TSMetric(name="filesProcessed", unit=None)] = 0
    bytes_processed: Ann[int, TSMetric(name="bytesProcessed", unit="Bytes")] = 0


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
_stats_queue: queue.Queue = queue.Queue()
_logger_lock: Lock = Lock()


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
        _stats_queue.put(StatsCounter(files_processed=1))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"processedFilePath": job_args["downloadInfo"]["destinationPath"]}


def run_job_insecure(job_args: JobArgs) -> None:
    destination_path = build_destination_path(job_args)

    if os.path.exists(destination_path):
        if os.stat(destination_path).st_size == job_args["downloadInfo"]["size"]:
            stream_log(
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
            stream_log(
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
            _stats_queue.put(StatsCounter(bytes_processed=chunk_size))

    if file_size != job_args["downloadInfo"]["size"]:
        raise Exception(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({file_size} B) size of downloaded file"
        )


def build_destination_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/{job_args["downloadInfo"]["destinationPath"]}'


def stream_log(log: AtmObject) -> None:
    with _logger_lock, open(f"/out/{LOGS_PIPED_RESULT_NAME}", "a") as f:
        json.dump(log, f)
        f.write("\n")


def monitor_jobs(heartbeat_callback: AtmHeartbeatCallback) -> None:
    any_job_ongoing = True
    while any_job_ongoing:
        any_job_ongoing = not _all_jobs_processed.wait(timeout=1)

        stats = StatsCounter()
        while not _stats_queue.empty():
            stats.update(_stats_queue.get())

        if stats:
            stream_measurements(stats.as_measurements())
            heartbeat_callback()


def stream_measurements(measurements: List[AtmTimeSeriesMeasurement]) -> None:
    with open(f"/out/{STATS_PIPED_RESULT_NAME}", "a") as f:
        for measurement in measurements:
            json.dump(measurement, f)
            f.write("\n")
