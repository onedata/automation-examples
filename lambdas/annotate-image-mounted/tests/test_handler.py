"""
Unit tests for the mounted image annotator.
"""

__author__ = "Lukasz Opiola, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs
from PIL import Image

from annotate_image_mounted import handler


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _job_args(file_type: str = "REG") -> dict[str, Any]:
    return {
        "file": {
            "fileId": "file-id",
            "type": file_type,
        }
    }


def test_annotates_image_with_xattrs(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _mounted_file(mount_point, "file-id")
    Image.new("RGB", (4, 2), color=(255, 0, 0)).save(target, format="PNG")
    stored: dict[str, bytes] = {}

    class XAttr:
        def __init__(self, path: str) -> None:
            assert Path(path) == target

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [None]
    assert stored == {
        "width": b"4",
        "height": b"2",
        "orientation": b"horizontal",
        "average_colour": b"red",
        "dominant_colour": b"red",
    }


def test_non_regular_file_is_ignored() -> None:
    rc = build_job_context(config={})

    results = handler.handle(build_jobs([_job_args(file_type="DIR")]), rc.context)

    assert results == [None]


def test_non_image_file_is_ignored(mount_point: Path) -> None:
    _mounted_file(mount_point, "file-id").write_text("not an image")

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [None]


def test_xattr_error_is_per_job_exception(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _mounted_file(mount_point, "file-id")
    Image.new("RGB", (4, 2), color=(255, 0, 0)).save(target, format="PNG")

    class XAttr:
        def __init__(self, path: str) -> None:
            pass

        def set(self, name: str, value: bytes) -> None:
            raise OSError("xattr failed")

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "Failed to set xattrs" in results[0]["exception"]
