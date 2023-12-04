"""A lambda which downloads files."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import os
import os.path
import queue
import traceback
from threading import Event, Thread
from typing import Final, Iterator, Union

import requests
from typing_extensions import TypedDict

from onedata_lambda_utils.logging import AtmLogger
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
from XRootD import client
from XRootD.client.flags import OpenFlags

##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"

DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2
EXTENDED_REST_REQUEST_TIMEOUT: Final[int] = 120


##===================================================================
## Lambda interface
##===================================================================


LOGS_STREAMER: Final[AtmLogger[AtmObject]] = AtmLogger(
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


class JobException(Exception):
    pass


_all_jobs_processed: Event = Event()
_measurements_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
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
    except (JobException, requests.RequestException) as ex:
        return AtmException(exception=str(ex))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {"processedFilePath": job_args["downloadInfo"]["destinationPath"]}


def run_job_insecure(job_args: JobArgs) -> None:
    destination_path = build_destination_path(job_args)

    if os.path.exists(destination_path):
        if os.stat(destination_path).st_size == job_args["downloadInfo"]["size"]:
            LOGS_STREAMER.info(
                {
                    "downloadInfo": job_args["downloadInfo"],
                    "message": (
                        "Skipping download as file with expected size "
                        "already exists at destination path."
                    ),
                }
            )
        else:
            LOGS_STREAMER.info(
                {
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
    if job_args["downloadInfo"]["sourceUrl"].startswith("root:/"):
        download_xrootd_file(job_args)
    else:
        download_http_file(job_args)


def download_xrootd_file(job_args: JobArgs) -> None:
    url = job_args["downloadInfo"]["sourceUrl"]

    with client.File() as fd:
        status, _ = fd.open(url, OpenFlags.READ)
        if not status.ok:
            raise JobException(
                f"Failed to open xrootd file at {url} due to: {status.message}"
            )

        data_stream = fd.readchunks(offset=0, chunksize=DOWNLOAD_CHUNK_SIZE)

        write_file(job_args, data_stream)


def download_http_file(job_args: JobArgs) -> None:
    request = requests.get(
        job_args["downloadInfo"]["sourceUrl"],
        stream=True,
        allow_redirects=True,
        timeout=EXTENDED_REST_REQUEST_TIMEOUT,
    )
    request.raise_for_status()

    write_file(job_args, request.iter_content(DOWNLOAD_CHUNK_SIZE))


def write_file(job_args: JobArgs, data_stream: Iterator[bytes]) -> None:
    file_size = 0
    destination_path = build_destination_path(job_args)
    with open(destination_path, "wb") as f:
        for chunk in data_stream:
            bytes_written = f.write(chunk)
            chunk_size = len(chunk)
            if bytes_written != chunk_size:
                raise JobException(
                    f"Unable to write a data chunk to file; written {bytes_written} bytes instead of {chunk_size}."
                )

            file_size += chunk_size
            _measurements_queue.put(BytesProcessed.build(value=chunk_size))

    if file_size != job_args["downloadInfo"]["size"]:
        raise JobException(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({file_size} B) size of download"
        )

    # serves as a double check that the Oneclient mounted as a sidecar has
    # coherent information about the file size after all chunks are written
    if os.path.getsize(destination_path) != job_args["downloadInfo"]["size"]:
        raise JobException(
            f"Mismatch between expected ({job_args['downloadInfo']['size']} B) "
            f"and actual ({os.path.getsize(destination_path)} B) size of the file "
            "stored in the target location."
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
