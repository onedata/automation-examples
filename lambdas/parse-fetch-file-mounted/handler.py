"""
A lambda which parses fetch files and returns list of files to download.

NOTE: fetch file is a file where every line has format: <url>\s+<size>\s+<path>
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import os.path
import traceback
from typing import Final, List, NamedTuple, Union

from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)
from typing_extensions import TypedDict

##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    fetchFile: AtmFile
    destinationDir: AtmFile


class FileDownloadInfo(TypedDict):
    sourceUrl: str
    destinationPath: str
    size: int


class JobResults(TypedDict):
    filesToDownload: List[FileDownloadInfo]
    statusLog: AtmObject


##===================================================================
## Lambda implementation
##===================================================================


class Job(NamedTuple):
    heartbeat_callback: AtmHeartbeatCallback
    args: JobArgs


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    job_results = []
    for job_args in job_batch_request["argsBatch"]:
        job = Job(args=job_args, heartbeat_callback=heartbeat_callback)
        job_results.append(run_job(job))

    return {"resultsBatch": job_results}


def run_job(job: Job) -> Union[AtmException, JobResults]:

    try:
        files_to_download = parse_fetch_file(job)
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            "filesToDownload": files_to_download,
            "statusLog": {
                "severity": "info",
                "fetchFileName": job.args["fetchFile"]["name"],
                "status": f"Found  {len(files_to_download)} files to be downloaded.",
            },
        }


def parse_fetch_file(job: Job) -> List[FileDownloadInfo]:
    fetch_file_path = "{mount_point}/.__onedata__file_id__{fetch_file_id}".format(
        mount_point=MOUNT_POINT, fetch_file_id=job.args["fetchFile"]["file_id"]
    )

    if os.path.isdir(fetch_file_path):
        return []

    files_to_download = []
    with open(fetch_file_path, "r") as f:
        for line in f:
            files_to_download.append(parse_line(job, line))
            job.heartbeat_callback()

    return files_to_download


def parse_line(job: Job, line: str) -> FileDownloadInfo:
    url, size, rel_path = line.strip().split()

    return {
        "sourceUrl": url,
        "destinationPath": ".__onedata__file_id__{root_dir_id}/{rel_path}".format(
            root_dir_id=job.args["destinationDir"]["file_id"],
            rel_path=rel_path.lstrip("/"),
        ),
        "size": int(size),
    }
