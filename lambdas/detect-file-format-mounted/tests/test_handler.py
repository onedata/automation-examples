"""
Unit tests for the mounted file format detector.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from detect_file_format_mounted import handler


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _job_args(
    *,
    file_name: str = "document.txt",
    file_type: str = "REG",
    metadata_key: str = "metadata",
) -> dict[str, Any]:
    return {
        "file": {
            "fileId": "file-id",
            "name": file_name,
            "type": file_type,
        },
        "metadataKey": metadata_key,
    }


def test_detects_format_and_stores_xattrs(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _mounted_file(mount_point, "file-id")
    target.write_text("content")
    stored: dict[str, bytes] = {}

    def from_file(path: str, mime: bool = False) -> str:
        assert Path(path) == target
        return "text/plain" if mime else "ASCII text"

    class XAttr:
        def __init__(self, path: str) -> None:
            assert Path(path) == target

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler.magic, "from_file", from_file)
    monkeypatch.setattr(handler.mimetypes, "guess_all_extensions", lambda mime_type: [".txt"])
    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [
        {
            "result": {
                "fileId": "file-id",
                "fileName": "document.txt",
                "formatName": "ASCII text",
                "mimeType": "text/plain",
                "extensions": [".txt"],
                "isExtensionMatchingFormat": True,
            }
        }
    ]
    assert stored == {
        "metadata.format-name": b"ASCII text",
        "metadata.mime-type": b"text/plain",
        "metadata.is-extension-matching-format": b"True",
    }


def test_reports_extension_mismatch(mount_point: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = _mounted_file(mount_point, "file-id")
    target.write_text("content")

    def from_file(path: str, mime: bool = False) -> str:
        assert Path(path) == target
        return "text/plain" if mime else "ASCII text"

    monkeypatch.setattr(handler.magic, "from_file", from_file)
    monkeypatch.setattr(handler.mimetypes, "guess_all_extensions", lambda mime_type: [".txt"])

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([_job_args(file_name="document.bin", metadata_key="")]),
        rc.context,
    )

    assert results[0]["result"]["extensions"] == [".txt"]
    assert results[0]["result"]["isExtensionMatchingFormat"] is False


def test_rejects_non_regular_file() -> None:
    rc = build_job_context(config={})

    results = handler.handle(build_jobs([_job_args(file_type="DIR")]), rc.context)

    assert "exception" in results[0]
    assert "Not a regular file" in results[0]["exception"]
