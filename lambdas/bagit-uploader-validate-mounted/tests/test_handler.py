"""
Unit tests for the mounted BagIt archive validator.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
import io
import tarfile
import zipfile
import zlib
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_sdk.testing import build_job_context, build_jobs

from bagit_uploader_validate_mounted.handler import handle


def _archive_file(file_id: str, name: str, file_type: str = "REG") -> dict[str, Any]:
    return {
        "fileId": file_id,
        "name": name,
        "type": file_type,
    }


def _job_args(file_id: str, name: str, file_type: str = "REG") -> dict[str, object]:
    return {"archive": _archive_file(file_id, name, file_type)}


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _bagit_entries(payload: bytes = b"hello bagit\n") -> dict[str, bytes]:
    checksum = hashlib.sha256(payload).hexdigest()
    return {
        "bag/bagit.txt": b"BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n",
        "bag/data/payload.txt": payload,
        "bag/manifest-sha256.txt": f"{checksum} data/payload.txt\n".encode(),
    }


def _write_zip_bag(path: Path, entries: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("bag/data/", b"")
        for name, content in entries.items():
            archive.writestr(name, content)


def _write_tar_bag(path: Path, entries: dict[str, bytes]) -> None:
    with tarfile.open(path, "w") as archive:
        data_dir = tarfile.TarInfo("bag/data")
        data_dir.type = tarfile.DIRTYPE
        archive.addfile(data_dir)

        for name, content in entries.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))


def test_valid_zip_archive(mount_point: Path) -> None:
    archive = _archive_file("zip-ok", "archive.zip")
    _write_zip_bag(_mounted_file(mount_point, "zip-ok"), _bagit_entries())

    rc = build_job_context(config={})
    results = handle(build_jobs([{"archive": archive}]), rc.context)

    assert results == [
        {
            "validArchives": [archive],
            "statusLog": {
                "archive": "archive.zip",
                "status": "Valid bagit archive",
            },
        }
    ]
    assert rc.heartbeats == 1


def test_valid_tar_archive(mount_point: Path) -> None:
    archive = _archive_file("tar-ok", "archive.tar")
    _write_tar_bag(_mounted_file(mount_point, "tar-ok"), _bagit_entries())

    rc = build_job_context(config={})
    results = handle(build_jobs([{"archive": archive}]), rc.context)

    assert results[0]["validArchives"] == [archive]
    assert results[0]["statusLog"]["status"] == "Valid bagit archive"


def test_valid_archive_with_adler32_manifest(mount_point: Path) -> None:
    payload = b"adler32 payload\n"
    checksum = format(zlib.adler32(payload, 1), "x")
    entries = {
        "bag/bagit.txt": (b"BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n"),
        "bag/data/payload.txt": payload,
        "bag/manifest-adler32.txt": f"{checksum} data/payload.txt\n".encode(),
    }
    archive = _archive_file("adler32-ok", "archive.zip")
    _write_zip_bag(_mounted_file(mount_point, "adler32-ok"), entries)

    rc = build_job_context(config={})
    results = handle(build_jobs([{"archive": archive}]), rc.context)

    assert results[0]["validArchives"] == [archive]
    assert results[0]["statusLog"]["status"] == "Valid bagit archive"


def test_invalid_archive_returns_status_log(mount_point: Path) -> None:
    _write_zip_bag(
        _mounted_file(mount_point, "missing-manifest"),
        {
            "bag/bagit.txt": (b"BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n"),
            "bag/data/payload.txt": b"hello bagit\n",
        },
    )

    rc = build_job_context(config={})
    results = handle(
        build_jobs([_job_args("missing-manifest", "invalid.zip")]),
        rc.context,
    )

    assert results == [
        {
            "validArchives": [],
            "statusLog": {
                "archive": "invalid.zip",
                "status": "Invalid bagit archive",
                "reason": "No manifest file found",
            },
        }
    ]


def test_payload_checksum_mismatch_is_ignored(mount_point: Path) -> None:
    archive = _archive_file("bad-checksum", "bad-checksum.zip")
    entries = _bagit_entries()
    entries["bag/manifest-sha256.txt"] = ("0" * 64 + " data/payload.txt\n").encode()
    _write_zip_bag(_mounted_file(mount_point, "bad-checksum"), entries)

    rc = build_job_context(config={})
    results = handle(
        build_jobs([{"archive": archive}]),
        rc.context,
    )

    assert results == [
        {
            "validArchives": [archive],
            "statusLog": {
                "archive": "bad-checksum.zip",
                "status": "Valid bagit archive",
            },
        }
    ]


def test_non_regular_file_is_per_job_exception() -> None:
    rc = build_job_context(config={})

    results = handle(build_jobs([_job_args("dir-id", "not-archive", "DIR")]), rc.context)

    assert "exception" in results[0]
    assert "Not an archive file" in results[0]["exception"]
