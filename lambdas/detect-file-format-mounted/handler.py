"""
A lambda which detects and reports file format based on file name and content.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import mimetypes
import os
import traceback
from typing import Final, List, NamedTuple, Union

from typing_extensions import TypedDict

import magic
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


class FileFormatReport(TypedDict):
    fileId: str
    fileName: str
    formatName: str
    mimeType: str
    extensions: List[str]
    isExtensionMatchingFormat: bool


class JobResults(TypedDict):
    result: FileFormatReport


##===================================================================
## Lambda implementation
##===================================================================


class FileFormat(NamedTuple):
    format_name: str
    mime_type: str
    extensions: List[str]


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
        file_format = get_file_format(job_args)
        does_extension_match_format = is_file_extension_and_format_matching(
            job_args, file_format
        )

        if job_args["metadataKey"]:
            set_file_format_xattrs(job_args, file_format, does_extension_match_format)
    except Exception:
        return AtmException(exception=traceback.format_exc())
    else:
        return {
            "result": {
                "fileId": job_args["file"]["file_id"],
                "fileName": job_args["file"]["name"],
                "formatName": file_format.format_name,
                "mimeType": file_format.mime_type,
                "extensions": file_format.extensions,
                "isExtensionMatchingFormat": does_extension_match_format,
            }
        }


def get_file_format(job_args: JobArgs) -> FileFormat:
    file_path = build_file_path(job_args)

    format_name = magic.from_file(file_path)
    mime_type = magic.from_file(file_path, mime=True)
    format_extensions = mimetypes.guess_all_extensions(mime_type)

    return FileFormat(
        format_name=format_name,
        mime_type=mime_type,
        extensions=format_extensions,
    )


def is_file_extension_and_format_matching(
    job_args: JobArgs, file_format: FileFormat
) -> bool:
    used_extension = os.path.splitext(job_args["file"]["name"])[1].lower()
    return len(file_format.extensions) == 0 or used_extension in file_format.extensions


def set_file_format_xattrs(
    job_args: JobArgs,
    file_format: FileFormat,
    is_extension_matching_format: bool,
) -> None:
    file_xattrs = xattr.xattr(build_file_path(job_args))

    xattrs_to_set = [
        ("format-name", file_format.format_name),
        ("mime-type", file_format.mime_type),
        ("is-extension-matching-format", str(is_extension_matching_format)),
    ]
    for xattr_subname, xattr_value in xattrs_to_set:
        xattr_name = f"{job_args['metadataKey']}.{xattr_subname}"
        file_xattrs.set(xattr_name, str.encode(xattr_value))


def build_file_path(job_args: JobArgs) -> str:
    return f'{MOUNT_POINT}/.__onedata__file_id__{job_args["file"]["file_id"]}'
