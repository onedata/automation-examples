"""
Unit tests for the mounted MIME type detector.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from detect_file_mime_type_mounted import handler


FILE_ID = "file-id"
FILE_NAME = "document.txt"
UNKNOWN_FILE_NAME = "unknown.extension-not-known"
METADATA_KEY = "metadata"
MIME_TYPE = "text/plain"
UNKNOWN_MIME_TYPE = "unknown"


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


def test_detects_mime_type_and_stores_xattr(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _mounted_file(mount_point, FILE_ID)
    target.write_text("content")
    stored: dict[str, bytes] = {}

    class XAttr:
        def __init__(self, path: str) -> None:
            assert Path(path) == target

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [
        {
            "format": {
                "fileId": FILE_ID,
                "fileName": FILE_NAME,
                "mimeType": MIME_TYPE,
            }
        }
    ]
    assert stored == {f"{METADATA_KEY}.mime-type": MIME_TYPE.encode()}


def test_unknown_mime_type_without_metadata(mount_point: Path) -> None:
    _mounted_file(mount_point, FILE_ID).write_text("content")

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([_job_args(file_name=UNKNOWN_FILE_NAME, metadata_key="")]),
        rc.context,
    )

    assert results[0]["format"]["mimeType"] == UNKNOWN_MIME_TYPE


def test_rejects_non_regular_file() -> None:
    rc = build_job_context(config={})

    results = handler.handle(build_jobs([_job_args(file_type="DIR")]), rc.context)

    assert "exception" in results[0]
    assert "Not a regular file" in results[0]["exception"]
