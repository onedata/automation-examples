"""
Unit tests for the mounted file format detector.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_sdk.testing import build_job_context, build_jobs

from detect_file_format_mounted import handler


FILE_ID = "file-id"
FILE_NAME = "document.txt"
MISMATCHED_FILE_NAME = "document.bin"
METADATA_KEY = "metadata"
FORMAT_NAME = "ASCII text"
MIME_TYPE = "text/plain"
EXTENSIONS = [".txt"]
EXTENSION_MATCHES = True


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
    file_name: str = FILE_NAME,
    file_type: str = "REG",
    metadata_key: str = METADATA_KEY,
) -> dict[str, Any]:
    return {
        "file": {
            "fileId": FILE_ID,
            "name": file_name,
            "type": file_type,
        },
        "metadataKey": metadata_key,
    }


def test_detects_format_and_stores_xattrs(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _mounted_file(mount_point, FILE_ID)
    target.write_text("content")
    stored: dict[str, bytes] = {}

    def from_file(path: str, mime: bool = False) -> str:
        assert Path(path) == target
        return MIME_TYPE if mime else FORMAT_NAME

    class XAttr:
        def __init__(self, path: str) -> None:
            assert Path(path) == target

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler.magic, "from_file", from_file)
    monkeypatch.setattr(handler.mimetypes, "guess_all_extensions", lambda mime_type: EXTENSIONS)
    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [
        {
            "result": {
                "fileId": FILE_ID,
                "fileName": FILE_NAME,
                "formatName": FORMAT_NAME,
                "mimeType": MIME_TYPE,
                "extensions": EXTENSIONS,
                "isExtensionMatchingFormat": EXTENSION_MATCHES,
            }
        }
    ]
    assert stored == {
        f"{METADATA_KEY}.format-name": FORMAT_NAME.encode(),
        f"{METADATA_KEY}.mime-type": MIME_TYPE.encode(),
        f"{METADATA_KEY}.is-extension-matching-format": str(EXTENSION_MATCHES).encode(),
    }


def test_reports_extension_mismatch(mount_point: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = _mounted_file(mount_point, FILE_ID)
    target.write_text("content")

    def from_file(path: str, mime: bool = False) -> str:
        assert Path(path) == target
        return MIME_TYPE if mime else FORMAT_NAME

    monkeypatch.setattr(handler.magic, "from_file", from_file)
    monkeypatch.setattr(handler.mimetypes, "guess_all_extensions", lambda mime_type: EXTENSIONS)

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([_job_args(file_name=MISMATCHED_FILE_NAME, metadata_key="")]),
        rc.context,
    )

    assert results[0]["result"]["extensions"] == EXTENSIONS
    assert results[0]["result"]["isExtensionMatchingFormat"] is False


def test_rejects_non_regular_file() -> None:
    rc = build_job_context(config={})

    results = handler.handle(build_jobs([_job_args(file_type="DIR")]), rc.context)

    assert "exception" in results[0]
    assert "Not a regular file" in results[0]["exception"]
