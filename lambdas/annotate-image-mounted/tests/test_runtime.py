"""
Integration tests for the mounted image annotator through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_sdk.testing import build_request, run_local
from PIL import Image

from annotate_image_mounted import handler


FILE_ID = "file-id"
FAILING_FILE_ID = "failing-file-id"
IMAGE_SIZE = (4, 2)
IMAGE_COLOUR = (255, 0, 0)
IMAGE_FORMAT = "PNG"
IMAGE_WIDTH = b"4"
IMAGE_HEIGHT = b"2"
IMAGE_ORIENTATION = b"horizontal"
IMAGE_COLOUR_NAME = b"red"


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def _job_args(file_id: str) -> dict[str, dict[str, str]]:
    return {
        "file": {
            "fileId": file_id,
            "type": "REG",
        }
    }


def test_run_end_to_end_isolates_per_job_xattr_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    target = _mounted_file(mount_point, FILE_ID)
    failing_target = _mounted_file(mount_point, FAILING_FILE_ID)
    Image.new("RGB", IMAGE_SIZE, color=IMAGE_COLOUR).save(target, format=IMAGE_FORMAT)
    Image.new("RGB", IMAGE_SIZE, color=IMAGE_COLOUR).save(failing_target, format=IMAGE_FORMAT)
    stored: dict[Path, dict[str, bytes]] = {}

    class XAttr:
        def __init__(self, path: str) -> None:
            self.path = Path(path)

        def set(self, name: str, value: bytes) -> None:
            if self.path == failing_target:
                raise OSError("xattr failed")
            stored.setdefault(self.path, {})[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    request = build_request(
        [_job_args(FILE_ID), _job_args(FAILING_FILE_ID)],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    batch = result.envelope["resultsBatch"]

    assert batch[0] is None
    assert "exception" in batch[1]
    assert "Failed to set xattrs" in batch[1]["exception"]
    assert stored[target] == {
        "width": IMAGE_WIDTH,
        "height": IMAGE_HEIGHT,
        "orientation": IMAGE_ORIENTATION,
        "average_colour": IMAGE_COLOUR_NAME,
        "dominant_colour": IMAGE_COLOUR_NAME,
    }
