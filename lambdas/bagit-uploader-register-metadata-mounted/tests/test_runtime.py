"""
Integration tests for the mounted BagIt metadata registrar through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_sdk.testing import build_request, run_local

from bagit_uploader_register_metadata_mounted import handler


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
            "name": "destination",
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

    target = _mounted_file(mount_point, DESTINATION_ID) / "file.txt"
    target.parent.mkdir()
    target.write_bytes(b"content")
    xattrs: dict[Path, dict[str, bytes]] = {}

    class XAttr:
        def __init__(self, path: Path) -> None:
            self.path = path

        def set(self, name: str, value: bytes) -> None:
            xattrs.setdefault(self.path, {})[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    with zipfile.ZipFile(_mounted_file(mount_point, ARCHIVE_ID), "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr("bag/manifest-sha256.txt", "abc123 data/file.txt\n")

    result = run_local(handler.handle, build_request([_job_args()], config={}), out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [None]}
    assert xattrs[target] == {"checksum.sha256.expected": b'"abc123"'}
