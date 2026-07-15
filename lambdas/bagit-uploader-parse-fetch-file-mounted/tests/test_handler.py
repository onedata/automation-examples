"""
Unit tests for the mounted BagIt fetch.txt parser.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

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


def _job_args(archive_type: str = "REG") -> dict[str, Any]:
    return {
        "archive": {"fileId": ARCHIVE_ID, "name": ARCHIVE_NAME, "type": archive_type},
        "destinationDir": {"fileId": DESTINATION_ID, "type": "DIR"},
    }


def test_parses_fetch_file(mount_point: Path) -> None:
    with zipfile.ZipFile(_mounted_file(mount_point, ARCHIVE_ID), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr(
            "bag/fetch.txt",
            f"{SOURCE_URL} {SOURCE_SIZE} data/{SOURCE_PATH}\n"
            f"{NESTED_SOURCE_URL} {NESTED_SOURCE_SIZE} data/{NESTED_SOURCE_PATH}\n",
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
                "archive": ARCHIVE_NAME,
                "status": "Found  2 files to be downloaded.",
            },
        }
    ]


def test_missing_fetch_file_returns_empty_list(mount_point: Path) -> None:
    with zipfile.ZipFile(_mounted_file(mount_point, ARCHIVE_ID), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert results[0]["filesToDownload"] == []


def test_rejects_path_outside_data(mount_point: Path) -> None:
    with zipfile.ZipFile(_mounted_file(mount_point, ARCHIVE_ID), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr("bag/fetch.txt", "https://example.test/a.txt 12 metadata/a.txt\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "File path not within data/" in results[0]["exception"]
