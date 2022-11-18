"""
A lambda which calculates (and saves as metadata) file checksum using REST interface.

NOTE: This lambda works on any type of file by simply returning `None` 
as checksum for anything but regular files.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import concurrent.futures
import dataclasses
import hashlib
import json
import os
import queue
import traceback
import zlib
from threading import Event, Thread
from typing import Final, Iterator, List, NamedTuple, Optional, Set, Union

import requests
from openfaas_lambda_utils.stats import AtmStatsCounter, TSMetric
from openfaas_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchRequestCtx,
    AtmJobBatchResponse,
    AtmTimeSeriesMeasurement,
)
from typing_extensions import Annotated as Ann
from typing_extensions import TypedDict


##===================================================================
## Lambda configuration
##===================================================================


AVAILABLE_CHECKSUM_ALGORITHMS: Final[Set[str]] = set().union(
    {"adler32"}, hashlib.algorithms_available
)
DOWNLOAD_CHUNK_SIZE: Final[int] = 10 * 1024**2
VERIFY_SSL_CERTS: Final[bool] = os.getenv("VERIFY_SSL_CERTIFICATES") != "false"


##===================================================================
## Lambda interface
##===================================================================


STATS_PIPED_RESULT_NAME: Final[str] = "stats"


@dataclasses.dataclass
class StatsCounter(AtmStatsCounter):
    files_processed: Ann[int, TSMetric(name="filesProcessed", unit=None)] = 0
    bytes_processed: Ann[int, TSMetric(name="bytesProcessed", unit="Bytes")] = 0


class JobArgs(TypedDict):
    file: AtmFile
    algorithm: str
    metadataKey: str


class FileChecksumReport(TypedDict):
    file_id: str
    algorithm: str
    checksum: Optional[str]


class JobResults(TypedDict):
    result: FileChecksumReport


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    ctx: AtmJobBatchRequestCtx
    args: JobArgs


_all_jobs_processed: Event = Event()
_stats_queue: queue.Queue = queue.Queue()


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    jobs_monitor = Thread(target=monitor_jobs, daemon=True, args=[heartbeat_callback])
    jobs_monitor.start()

    jobs = [
        Job(args=job_args, ctx=job_batch_request["ctx"])
        for job_args in job_batch_request["argsBatch"]
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        job_results = list(executor.map(run_job, jobs))

    _all_jobs_processed.set()
    jobs_monitor.join()

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults]:
    if job.args["file"]["type"] != "REG":
        return build_job_results(job, None)

    if job.args["algorithm"] not in AVAILABLE_CHECKSUM_ALGORITHMS:
        return AtmException(
            exception=(
                f"{job.args['algorithm']} algorithm is unsupported."
                f"Available ones are: {AVAILABLE_CHECKSUM_ALGORITHMS}"
            )
        )

    try:
        data_stream = get_file_data_stream(job)
        checksum = calculate_checksum(job, data_stream)

        if job.args["metadataKey"] != "":
            set_file_metadata(job, checksum)

        _stats_queue.put(StatsCounter(files_processed=1))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return build_job_results(job, checksum)


def build_job_results(job: Job, checksum: Optional[str]) -> JobResults:
    return {
        "result": {
            "file_id": job.args["file"]["file_id"],
            "algorithm": job.args["algorithm"],
            "checksum": checksum,
        }
    }


def get_file_data_stream(job: Job) -> Iterator[bytes]:
    response = requests.get(
        build_file_rest_url(job, "content"),
        headers={"x-auth-token": job.ctx["accessToken"]},
        stream=True,
        verify=VERIFY_SSL_CERTS,
    )
    response.raise_for_status()

    return response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE)


def calculate_checksum(job: Job, data_stream: Iterator[bytes]) -> str:
    algorithm = job.args["algorithm"]

    if algorithm == "adler32":
        value = 1
        for data in data_stream:
            value = zlib.adler32(data, value)
            _stats_queue.put(StatsCounter(bytes_processed=len(data)))
        return format(value, "x")
    else:
        hash = getattr(hashlib, algorithm)()
        for data in data_stream:
            hash.update(data)
            _stats_queue.put(StatsCounter(bytes_processed=len(data)))
        return hash.hexdigest()


def set_file_metadata(job: Job, checksum: str) -> None:
    response = requests.put(
        build_file_rest_url(job, "metadata/xattrs"),
        headers={
            "x-auth-token": job.ctx["accessToken"],
            "content-type": "application/json",
        },
        json={job.args["metadataKey"]: checksum},
        verify=VERIFY_SSL_CERTS,
    )
    response.raise_for_status()


def build_file_rest_url(job: Job, subpath: str) -> str:
    return "https://{domain}/api/v3/oneprovider/data/{file_id}/{subpath}".format(
        domain=job.ctx["oneproviderDomain"],
        file_id=job.args["file"]["file_id"],
        subpath=subpath.lstrip("/"),
    )


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
