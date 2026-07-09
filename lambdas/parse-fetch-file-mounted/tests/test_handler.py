"""
Unit tests for the mounted fetch file parser.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from parse_fetch_file_mounted.handler import handle


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _job_args(fetch_file_type: str = "REG") -> dict[str, Any]:
    return {
        "fetchFile": {
            "fileId": "fetch-file-id",
            "name": "fetch.txt",
            "type": fetch_file_type,
        },
        "destinationDir": {
            "fileId": "destination-id",
            "type": "DIR",
        },
    }


def test_parses_fetch_file(mount_point: Path) -> None:
    _mounted_file(mount_point, "fetch-file-id").write_text(
        "https://example.test/a.txt 12 a.txt\nroot://example.test/b.bin 34 nested/b.bin\n"
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
                "fetchFileName": "fetch.txt",
                "status": "Found  2 files to be downloaded.",
            },
        }
    ]


def test_directory_fetch_file_returns_empty_list(mount_point: Path) -> None:
    _mounted_file(mount_point, "fetch-file-id").mkdir()

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args(fetch_file_type="DIR")]), rc.context)

    assert results[0]["filesToDownload"] == []
    assert results[0]["statusLog"]["status"] == "Found  0 files to be downloaded."


def test_rejects_malformed_line(mount_point: Path) -> None:
    _mounted_file(mount_point, "fetch-file-id").write_text("https://example.test/a.txt 12\n")

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "line number 1" in results[0]["exception"]


def test_rejects_unsafe_destination_path(mount_point: Path) -> None:
    _mounted_file(mount_point, "fetch-file-id").write_text(
        "https://example.test/a.txt 12 ../a.txt\n"
    )

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "Unsafe destination path" in results[0]["exception"]
