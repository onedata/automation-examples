"""
A lambda which detects and reports file mime format based on file name.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import mimetypes
import traceback
from typing import Final, Union

from typing_extensions import TypedDict

import xattr
from onedata_lambda_utils.types import (
    AtmException,
    AtmFile,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)

##===================================================================
## Lambda configuration
##===================================================================


MOUNT_POINT: Final[str] = "/mnt/onedata"


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    file: AtmFile
    metadataKey: str


class FileMimeFormatReport(TypedDict):
    fileId: str
    fileName: str
    mimeType: str


class JobResults(TypedDict):
    format: FileMimeFormatReport


##===================================================================
## Lambda implementation
##===================================================================


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    results = []
    for job_args in job_batch_request["argsBatch"]:
        results.append(run_job(job_args))
        heartbeat_callback()

    return {"resultsBatch": results}


def run_job(job_args: JobArgs) -> Union[AtmException, JobResults]:
    if not job_args["file"]["type"] == "REG":
        return AtmException(exception="Not a regular file")

    try:
        mime_type = get_mime_filename_type(job_args["file"]["name"])

        if metadata_key := job_args["metadataKey"]:
            xattr_data = xattr.xattr(build_file_path(job_args))
            xattr_data.set(f"{metadata_key}.mime-type", str.encode(mime_type))
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            "format": {
                "fileId": job_args["file"]["file_id"],
                "fileName": job_args["file"]["name"],
                "mimeType": mime_type,
            }
        }


def get_mime_filename_type(file_path: str) -> str:
    file_type, _ = mimetypes.guess_type(file_path, strict=True)
    return "unknown" if file_type is None else file_type


def build_file_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job_args["file"]["file_id"]}'
