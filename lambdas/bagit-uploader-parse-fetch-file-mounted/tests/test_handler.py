"""
Unit tests for the mounted BagIt fetch.txt parser.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from bagit_uploader_parse_fetch_file_mounted.handler import handle


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _job_args(archive_type: str = "REG") -> dict[str, Any]:
    return {
        "archive": {"fileId": "archive-id", "name": "archive.zip", "type": archive_type},
        "destinationDir": {"fileId": "destination-id", "type": "DIR"},
    }


def test_parses_fetch_file(mount_point: Path) -> None:
    with zipfile.ZipFile(_mounted_file(mount_point, "archive-id"), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr(
            "bag/fetch.txt",
            "https://example.test/a.txt 12 data/a.txt\n"
            "root://example.test/b.bin 34 data/nested/b.bin\n",
        )

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert results == [
        {
            "filesToDownload": [
                {
                    "sourceUrl": "https://example.test/a.txt",
                    "destinationPath": ".__onedata__file_id__destination-id/a.txt",
                    "size": 12,
                },
                {
                    "sourceUrl": "root://example.test/b.bin",
                    "destinationPath": ".__onedata__file_id__destination-id/nested/b.bin",
                    "size": 34,
                },
            ],
            "statusLog": {
                "severity": "info",
                "archive": "archive.zip",
                "status": "Found  2 files to be downloaded.",
            },
        }
    ]


def test_missing_fetch_file_returns_empty_list(mount_point: Path) -> None:
    with zipfile.ZipFile(_mounted_file(mount_point, "archive-id"), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert results[0]["filesToDownload"] == []


def test_rejects_path_outside_data(mount_point: Path) -> None:
    with zipfile.ZipFile(_mounted_file(mount_point, "archive-id"), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr("bag/fetch.txt", "https://example.test/a.txt 12 metadata/a.txt\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "File path not within data/" in results[0]["exception"]
