"""
Integration tests for the mounted fetch file parser through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from parse_fetch_file_mounted.handler import handle


FETCH_FILE_ID = "fetch-file-id"
DESTINATION_ID = "destination-id"
FETCH_FILE_NAME = "fetch.txt"
SOURCE_URL = "https://example.test/a.txt"
SOURCE_SIZE = 12
SOURCE_PATH = "a.txt"
NESTED_SOURCE_URL = "root://example.test/b.bin"
NESTED_SOURCE_SIZE = 34
NESTED_SOURCE_PATH = "nested/b.bin"


def _job_args() -> dict[str, Any]:
    return {
        "fetchFile": {
            "fileId": FETCH_FILE_ID,
            "name": FETCH_FILE_NAME,
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

    _mounted_file(mount_point, FETCH_FILE_ID).write_text(
        f"{SOURCE_URL} {SOURCE_SIZE} {SOURCE_PATH}\n"
        f"{NESTED_SOURCE_URL} {NESTED_SOURCE_SIZE} {NESTED_SOURCE_PATH}\n"
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
                    "fetchFileName": FETCH_FILE_NAME,
                    "status": "Found  2 files to be downloaded.",
                },
            }
        ]
    }
