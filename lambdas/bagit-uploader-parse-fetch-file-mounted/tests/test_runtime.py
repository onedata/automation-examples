"""
Integration tests for the mounted BagIt fetch.txt parser through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from bagit_uploader_parse_fetch_file_mounted.handler import handle


ARCHIVE_ID = "archive-id"
ARCHIVE_NAME = "archive.zip"
DESTINATION_ID = "destination-id"
SOURCE_URL = "https://example.test/a.txt"
SOURCE_SIZE = 12
SOURCE_PATH = "a.txt"
NESTED_SOURCE_URL = "root://example.test/b.bin"
NESTED_SOURCE_SIZE = 34
NESTED_SOURCE_PATH = "nested/b.bin"


def _job_args() -> dict[str, Any]:
    return {
        "archive": {
            "fileId": ARCHIVE_ID,
            "name": ARCHIVE_NAME,
            "type": "REG",
        },
        "destinationDir": {
            "fileId": DESTINATION_ID,
            "type": "DIR",
        },
    }


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _destination_path(rel_path: str) -> str:
    return f".__onedata__file_id__{DESTINATION_ID}/{rel_path}"


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with zipfile.ZipFile(_mounted_file(mount_point, ARCHIVE_ID), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr(
            "bag/fetch.txt",
            f"{SOURCE_URL} {SOURCE_SIZE} data/{SOURCE_PATH}\n"
            f"{NESTED_SOURCE_URL} {NESTED_SOURCE_SIZE} data/{NESTED_SOURCE_PATH}\n",
        )

    result = run_local(handle, build_request([_job_args()], config={}), out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "filesToDownload": [
                    {
                        "sourceUrl": SOURCE_URL,
                        "destinationPath": _destination_path(SOURCE_PATH),
                        "size": SOURCE_SIZE,
                    },
                    {
                        "sourceUrl": NESTED_SOURCE_URL,
                        "destinationPath": _destination_path(NESTED_SOURCE_PATH),
                        "size": NESTED_SOURCE_SIZE,
                    },
                ],
                "statusLog": {
                    "severity": "info",
                    "archive": ARCHIVE_NAME,
                    "status": "Found  2 files to be downloaded.",
                },
            }
        ]
    }
