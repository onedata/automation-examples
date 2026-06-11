"""
A lambda which detects a file's format from its name and content (libmagic) and optionally
stores it as the file's metadata, using a mounted Oneclient. Non-regular files fail the job.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import mimetypes
import os
from typing import NamedTuple, TypedDict

import magic
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


class FileFormatReport(TypedDict):
    fileId: str
    fileName: str
    formatName: str
    mimeType: str
    extensions: list[str]
    isExtensionMatchingFormat: bool


class JobResult(TypedDict):
    result: FileFormatReport


##===================================================================
## Lambda implementation
##===================================================================


class _FileFormat(NamedTuple):
    format_name: str
    mime_type: str
    extensions: list[str]


@per_job
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    """Report one file's format (libmagic, by content)."""
    file = job.args["file"]
    if file["type"] != "REG":
        raise JobException("Not a regular file")

    file_path = mounted_file_path(file["fileId"])
    file_format = _detect_format(file_path)
    extension_matches = _extension_matches_format(file["name"], file_format)

    if metadata_key := job.args["metadataKey"]:
        _store_format_xattrs(file_path, metadata_key, file_format, extension_matches)

    return {
        "result": {
            "fileId": file["fileId"],
            "fileName": file["name"],
            "formatName": file_format.format_name,
            "mimeType": file_format.mime_type,
            "extensions": file_format.extensions,
            "isExtensionMatchingFormat": extension_matches,
        }
    }


def _detect_format(file_path: str) -> _FileFormat:
    mime_type = magic.from_file(file_path, mime=True)
    return _FileFormat(
        format_name=magic.from_file(file_path),
        mime_type=mime_type,
        extensions=mimetypes.guess_all_extensions(mime_type),
    )


def _extension_matches_format(file_name: str, file_format: _FileFormat) -> bool:
    used_extension = os.path.splitext(file_name)[1].lower()
    return len(file_format.extensions) == 0 or used_extension in file_format.extensions


def _store_format_xattrs(
    file_path: str, metadata_key: str, file_format: _FileFormat, extension_matches: bool
) -> None:
    entries = [
        ("format-name", file_format.format_name),
        ("mime-type", file_format.mime_type),
        ("is-extension-matching-format", str(extension_matches)),
    ]
    try:
        file_xattrs = xattr.xattr(file_path)
        for subname, value in entries:
            file_xattrs.set(f"{metadata_key}.{subname}", value.encode())
    except OSError as ex:
        raise JobException(
            f"Failed to set format xattrs under {metadata_key!r} on the file: {ex}"
        ) from ex
