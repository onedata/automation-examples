"""
A lambda which detects a file's MIME type from its name (not its content) and optionally
stores it as the file's metadata, using a mounted Oneclient. Non-regular files fail the job.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import mimetypes
from typing import TypedDict

import xattr
from onedata_lambda_utils import (
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    mounted_file_path,
    per_job,
)


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


class JobResult(TypedDict):
    format: FileMimeFormatReport


##===================================================================
## Lambda implementation
##===================================================================


@per_job
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    """Report one file's MIME type."""
    file = job.args["file"]
    if file["type"] != "REG":
        raise JobException("Not a regular file")

    mime_type = _guess_mime_type(file["name"])
    if metadata_key := job.args["metadataKey"]:
        _store_mime_type_xattr(file["fileId"], metadata_key, mime_type)

    return {
        "format": {
            "fileId": file["fileId"],
            "fileName": file["name"],
            "mimeType": mime_type,
        }
    }


def _guess_mime_type(file_name: str) -> str:
    mime_type, _ = mimetypes.guess_type(file_name, strict=True)
    return "unknown" if mime_type is None else mime_type


def _store_mime_type_xattr(file_id: str, metadata_key: str, mime_type: str) -> None:
    try:
        xattr.xattr(mounted_file_path(file_id)).set(f"{metadata_key}.mime-type", mime_type.encode())
    except OSError as ex:
        raise JobException(
            f"Failed to set xattr {metadata_key!r}.mime-type on the file: {ex}"
        ) from ex
