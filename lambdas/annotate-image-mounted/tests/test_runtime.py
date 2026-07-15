"""
Integration tests for the mounted image annotator through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local
from PIL import Image

from annotate_image_mounted import handler


FILE_ID = "file-id"
IMAGE_SIZE = (4, 2)
IMAGE_COLOUR = (255, 0, 0)
IMAGE_FORMAT = "PNG"
IMAGE_WIDTH = b"4"
IMAGE_HEIGHT = b"2"
IMAGE_ORIENTATION = b"horizontal"
IMAGE_COLOUR_NAME = b"red"


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    target = mount_point / f".__onedata__file_id__{FILE_ID}"
    Image.new("RGB", IMAGE_SIZE, color=IMAGE_COLOUR).save(target, format=IMAGE_FORMAT)
    stored: dict[str, bytes] = {}

    class XAttr:
        def __init__(self, path: str) -> None:
            assert Path(path) == target

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    request = build_request(
        [
            {
                "file": {
                    "fileId": FILE_ID,
                    "type": "REG",
                }
            }
        ],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [None]}
    assert stored == {
        "width": IMAGE_WIDTH,
        "height": IMAGE_HEIGHT,
        "orientation": IMAGE_ORIENTATION,
        "average_colour": IMAGE_COLOUR_NAME,
        "dominant_colour": IMAGE_COLOUR_NAME,
    }
