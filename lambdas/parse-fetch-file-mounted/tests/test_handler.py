"""
Unit tests for the mounted fetch file parser.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

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


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _destination_path(rel_path: str) -> str:
    return f".__onedata__file_id__{DESTINATION_ID}/{rel_path}"


def _job_args(fetch_file_type: str = "REG") -> dict[str, Any]:
    return {
        "fetchFile": {
            "fileId": FETCH_FILE_ID,
            "name": FETCH_FILE_NAME,
            "type": fetch_file_type,
        },
        "destinationDir": {
            "fileId": DESTINATION_ID,
            "type": "DIR",
        },
    }


def test_parses_fetch_file(mount_point: Path) -> None:
    _mounted_file(mount_point, FETCH_FILE_ID).write_text(
        f"{SOURCE_URL} {SOURCE_SIZE} {SOURCE_PATH}\n"
        f"{NESTED_SOURCE_URL} {NESTED_SOURCE_SIZE} {NESTED_SOURCE_PATH}\n"
    )

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert results == [
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


def test_directory_fetch_file_returns_empty_list(mount_point: Path) -> None:
    _mounted_file(mount_point, FETCH_FILE_ID).mkdir()

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args(fetch_file_type="DIR")]), rc.context)

    assert results[0]["filesToDownload"] == []
    assert results[0]["statusLog"]["status"] == "Found  0 files to be downloaded."


def test_rejects_malformed_line(mount_point: Path) -> None:
    _mounted_file(mount_point, FETCH_FILE_ID).write_text("https://example.test/a.txt 12\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "line number 1" in results[0]["exception"]


def test_rejects_unsafe_destination_path(mount_point: Path) -> None:
    _mounted_file(mount_point, FETCH_FILE_ID).write_text("https://example.test/a.txt 12 ../a.txt\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "Unsafe destination path" in results[0]["exception"]
