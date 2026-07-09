"""
Unit tests for the mounted BagIt metadata registrar.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from bagit_uploader_register_metadata_mounted import handler


def _job_args(
    archive_id: str = "archive-id",
    archive_name: str = "archive.zip",
    destination_id: str = "destination-id",
    archive_type: str = "REG",
    destination_type: str = "DIR",
) -> dict[str, Any]:
    return {
        "archive": {
            "fileId": archive_id,
            "name": archive_name,
            "type": archive_type,
        },
        "destinationDir": {
            "fileId": destination_id,
            "name": "destination",
            "type": destination_type,
        },
    }


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _write_zip_archive(path: Path, manifest_content: str) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr("bag/manifest-sha256.txt", manifest_content)


@pytest.fixture
def xattrs(monkeypatch: pytest.MonkeyPatch) -> dict[Path, dict[str, bytes]]:
    stored_xattrs: dict[Path, dict[str, bytes]] = {}

    class XAttr:
        def __init__(self, path: Path) -> None:
            self.path = path

        def set(self, name: str, value: bytes) -> None:
            stored_xattrs.setdefault(self.path, {})[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)
    return stored_xattrs


def test_registers_expected_checksum_xattrs(
    mount_point: Path, xattrs: dict[Path, dict[str, bytes]]
) -> None:
    target = _mounted_file(mount_point, "destination-id") / "nested" / "file.txt"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"content")
    _write_zip_archive(
        _mounted_file(mount_point, "archive-id"),
        "abc123 data/nested/file.txt\n",
    )

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [None]
    assert xattrs[target] == {"checksum.sha256.expected": b'"abc123"'}
    assert rc.heartbeats == 1


def test_rejects_manifest_path_outside_data(mount_point: Path) -> None:
    _write_zip_archive(
        _mounted_file(mount_point, "archive-id"),
        "abc123 metadata/file.txt\n",
    )

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "Manifest path must point inside data/" in results[0]["exception"]


def test_rejects_non_regular_archive() -> None:
    rc = build_job_context(config={})

    results = handler.handle(build_jobs([_job_args(archive_type="DIR")]), rc.context)

    assert "exception" in results[0]
    assert "Archive must be a regular file" in results[0]["exception"]
