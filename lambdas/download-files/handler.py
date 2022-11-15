"""A lambda which downloads files."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import json
import os
import os.path
import queue
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Final, Union

import requests
from openfaas_lambda_utils.types import (
    AtmException,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import TypedDict


class DownloadInfo(TypedDict):
    sourceUrl: str
    destinationPath: str
    size: int


class AtmJobArgs(TypedDict):
    downloadInfo: DownloadInfo


class AtmJobResults(TypedDict):
    processedFilePath: str


@dataclass
class StatsCounter:
    file_processed: int = 0
    bytes_processed: int = 0

    def merge(self, other: "StatsCounter") -> None:
        self.file_processed += other.file_processed
        self.bytes_processed += other.bytes_processed


HTTP_GET_TIMEOUT_SEC: Final[int] = 120
DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2


def handle(
    job_batch_request: AtmJobBatchRequest[AtmJobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[AtmJobResults]:

    jobs_monitor = threading.Thread(target=monitor_jobs, args=[heartbeat_callback])
    jobs_monitor.daemon = True
    jobs_monitor.start()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(run_job, job_batch_request["argsBatch"]))

    _all_jobs_processed.set()
    _all_stats_streamed.wait()

    return {"resultsBatch": results}


def run_job(job_args: AtmJobArgs) -> Union[AtmJobResults, AtmException]:
    try:
        run_job_insecure(job_args)
        _stats_queue.put(StatsCounter(file_processed=1))

        return {"processedFilePath": job_args["downloadInfo"]["destinationPath"]}
    except Exception:
        return AtmException(exception=traceback.format_exc())


def run_job_insecure(job_args: AtmJobArgs) -> None:
    destination_path = build_destination_path(job_args)

    if os.path.exists(destination_path):
        if os.stat(destination_path).st_size == job_args["downloadInfo"]["size"]:
            stream_log(
                {
                    "severity": "info",
                    "destinationPath": destination_path,
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
                    "destinationPath": destination_path,
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


def download_file(job_args: AtmJobArgs) -> None:
    r = requests.get(
        job_args["downloadInfo"]["sourceUrl"],
        stream=True,
        allow_redirects=True,
        timeout=HTTP_GET_TIMEOUT_SEC,
    )
    r.raise_for_status()

    with open(build_destination_path(job_args), "wb") as f:
        for chunk in r.iter_content(DOWNLOAD_CHUNK_SIZE):
            f.write(chunk)
            _stats_queue.put(StatsCounter(bytes_processed=len(chunk)))

    assert_proper_file_size(job_args)


def assert_proper_file_size(job_args: AtmJobArgs) -> None:
    time.sleep(2)
    file_size = os.stat(build_destination_path(job_args)).st_size

    if file_size != job_args["downloadInfo"]["size"]:
        raise Exception(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({file_size} B) size of downloaded file"
        )


def build_destination_path(job_args: AtmJobArgs) -> str:
    return f'/mnt/onedata/{job_args["downloadInfo"]["destinationPath"]}'


_logger_lock: threading.Lock = threading.Lock()


def stream_log(log: AtmObject) -> None:
    with _logger_lock, open("/out/logs", "a") as f:
        json.dump(log, f)
        f.write("\n")


_all_jobs_processed: threading.Event = threading.Event()
_all_stats_streamed: threading.Event = threading.Event()
_stats_queue: queue.Queue = queue.Queue()


def monitor_jobs(heartbeat_callback: AtmHeartbeatCallback) -> None:
    all_jobs_processed = False

    while not all_jobs_processed:
        all_jobs_processed = _all_jobs_processed.wait(timeout=1)

        stats = StatsCounter()
        while not _stats_queue.empty():
            stats.merge(_stats_queue.get())

        if (stats.file_processed, stats.bytes_processed) != (0, 0):
            heartbeat_callback()
            stream_measurements(stats)

    _all_stats_streamed.set()


def stream_measurements(stats: StatsCounter) -> None:
    with open("/out/stats", "a") as f:
        if stats.file_processed > 0:
            json.dump(build_measurement("filesProcessed", stats.file_processed), f)
            f.write("\n")
        if stats.bytes_processed > 0:
            json.dump(build_measurement("bytesProcessed", stats.bytes_processed), f)
            f.write("\n")


def build_measurement(ts_name: str, value: int) -> AtmTimeSeriesMeasurement:
    return {
        "tsName": ts_name,
        "value": value,
        "timestamp": int(time.time()),
    }
