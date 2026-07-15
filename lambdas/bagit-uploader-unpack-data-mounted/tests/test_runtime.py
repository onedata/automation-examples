"""
Integration tests for the mounted BagIt data unpacker through the real SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from bagit_uploader_unpack_data_mounted.handler import handle


ARCHIVE_ID = "archive-id"
ARCHIVE_NAME = "archive.zip"
DESTINATION_ID = "destination-id"


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


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with zipfile.ZipFile(_mounted_file(mount_point, ARCHIVE_ID), "w") as archive:
        archive.writestr("bag/data/", b"")
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr("bag/data/file.txt", b"runtime content")

    request = build_request([_job_args()], config={})
    result = run_local(handle, request, out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "unpackedFiles": [f".__onedata__file_id__{DESTINATION_ID}/file.txt"],
                "statusLog": {
                    "archive": ARCHIVE_NAME,
                    "status": "Successfully unpacked 1 files.",
                },
            }
        ]
    }
    assert _mounted_file(mount_point, DESTINATION_ID).joinpath("file.txt").read_bytes() == (
        b"runtime content"
    )
    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("bytesUnpacked") == 1
    assert ts_names.count("filesUnpacked") == 1
