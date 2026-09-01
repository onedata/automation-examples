"""
Unit tests for the parallel mounted BagIt data unpacker variant.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import zipfile
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_sdk.testing import build_job_context, build_jobs

from bagit_uploader_unpack_data_mounted.handler import handle


ARCHIVE_ID = "archive-id"
ARCHIVE_NAME = "archive.zip"
DESTINATION_ID = "destination-id"


def _job_args(
    archive_id: str = ARCHIVE_ID,
    archive_name: str = ARCHIVE_NAME,
    destination_id: str = DESTINATION_ID,
) -> dict[str, Any]:
    return {
        "archive": {
            "fileId": archive_id,
            "name": archive_name,
            "type": "REG",
        },
        "destinationDir": {
            "fileId": destination_id,
            "type": "DIR",
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


def _write_zip_archive(path: Path, entries: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("bag/data/", b"")
        for name, content in entries.items():
            archive.writestr(name, content)


def test_parallel_variant_unpacks_zip_data_files(mount_point: Path) -> None:
    archive_path = _mounted_file(mount_point, ARCHIVE_ID)
    destination_path = _mounted_file(mount_point, DESTINATION_ID)
    _write_zip_archive(
        archive_path,
        {
            "bag/bagit.txt": b"BagIt-Version: 0.97\n",
            "bag/data/file.txt": b"hello",
            "bag/data/nested/file.bin": b"nested bytes",
            "bag/tag.txt": b"ignored",
        },
    )

    rc = build_job_context(config={})
    results = handle(build_jobs([_job_args()]), rc.context)

    assert results == [
        {
            "unpackedFiles": [
                f".__onedata__file_id__{DESTINATION_ID}/file.txt",
                f".__onedata__file_id__{DESTINATION_ID}/nested/file.bin",
            ],
            "statusLog": {
                "archive": ARCHIVE_NAME,
                "status": "Successfully unpacked 2 files.",
            },
        }
    ]
    assert (destination_path / "file.txt").read_bytes() == b"hello"
    assert (destination_path / "nested/file.bin").read_bytes() == b"nested bytes"


def test_parallel_variant_rejects_unsafe_archive_path(mount_point: Path) -> None:
    _write_zip_archive(
        _mounted_file(mount_point, "unsafe-id"),
        {
            "bag/bagit.txt": b"BagIt-Version: 0.97\n",
            "bag/data/../outside.txt": b"bad",
        },
    )

    rc = build_job_context(config={})
    results = handle(
        build_jobs([_job_args(archive_id="unsafe-id", archive_name="unsafe.zip")]),
        rc.context,
    )

    assert "exception" in results[0]
    assert "Unsafe archive path" in results[0]["exception"]
    assert not (mount_point / "outside.txt").exists()
