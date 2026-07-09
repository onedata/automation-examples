"""
Integration tests for the mounted BagIt validator through the real SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from bagit_uploader_validate_mounted.handler import handle


def _archive_file(file_id: str, name: str, file_type: str = "REG") -> dict[str, Any]:
    return {
        "fileId": file_id,
        "name": name,
        "type": file_type,
    }


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _write_zip_bag(path: Path, payload: bytes = b"runtime bagit\n") -> None:
    checksum = hashlib.sha256(payload).hexdigest()
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("bag/data/", b"")
        archive.writestr(
            "bag/bagit.txt",
            b"BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n",
        )
        archive.writestr("bag/data/payload.txt", payload)
        archive.writestr("bag/manifest-sha256.txt", f"{checksum} data/payload.txt\n")


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))

    valid_archive = _archive_file("valid-id", "valid.zip")
    _write_zip_bag(_mounted_file(mount_point, "valid-id"))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    request = build_request(
        [
            {"archive": valid_archive},
            {"archive": _archive_file("dir-id", "not-archive", "DIR")},
        ],
        config={},
    )
    result = run_local(handle, request, out_dir=out_dir)

    batch = result.envelope["resultsBatch"]
    assert batch[0] == {
        "validArchives": [valid_archive],
        "statusLog": {
            "archive": "valid.zip",
            "status": "Valid bagit archive",
        },
    }
    assert "exception" in batch[1]
    assert "Not an archive file" in batch[1]["exception"]
