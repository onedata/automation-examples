"""
A lambda which parses fetch files and returnes list of files to fetch informations.

NOTE: fetch file is a file where every line has format: <url>\s+<size>\s+<path>
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import traceback
from typing import List, Union

from openfaas_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)
from typing_extensions import TypedDict


class AtmJobArgs(TypedDict):
    fetchFile: AtmFile
    destinationDir: AtmFile


class AtmFileToFetchInfo(TypedDict):
    sourceUrl: str
    destinationPath: str
    size: int


class AtmJobResults(TypedDict):
    filesToFetch: List[AtmFileToFetchInfo]
    statusLog: AtmObject


def handle(
    job_batch_request: AtmJobBatchRequest[AtmJobArgs],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[AtmJobResults]:

    job_results = []
    for job_args in job_batch_request["argsBatch"]:
        job_results.append(run_job(job_args, heartbeat_callback))

    return {"resultsBatch": job_results}


def run_job(
    job_args: AtmJobArgs, heartbeat_callback: AtmHeartbeatCallback
) -> Union[AtmException, AtmJobResults]:

    try:
        files_to_fetch = parse_fetch_file(job_args, heartbeat_callback)
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            "filesToFetch": files_to_fetch,
            "statusLog": {
                "severity": "info",
                "fetchFileName": job_args["fetchFile"]["name"],
                "status": f"Found  {len(files_to_fetch)} files to be fetched.",
            },
        }


def parse_fetch_file(
    job_args: AtmJobArgs, heartbeat_callback: AtmHeartbeatCallback
) -> List[AtmFileToFetchInfo]:

    if job_args["fetchFile"]["type"] != "REG":
        return []

    fetch_file_path = "/mnt/onedata/.__onedata__file_id__{fetch_file_id}".format(
        fetch_file_id=job_args["fetchFile"]["file_id"]
    )
    files_to_fetch = []

    with open(fetch_file_path, "r") as f:
        for line in f:
            files_to_fetch.append(parse_line(job_args, line))
            heartbeat_callback()

    return files_to_fetch


def parse_line(job_args: AtmJobArgs, line: str) -> AtmFileToFetchInfo:
    url, size, path = line.strip().split()

    return {
        "sourceUrl": url,
        "destinationPath": ".__onedata__file_id__{root_dir_id}/{rel_path}".format(
            root_dir_id=job_args["destinationDir"]["file_id"],
            rel_path=path[len("data/") :],
        ),
        "size": int(size),
    }
