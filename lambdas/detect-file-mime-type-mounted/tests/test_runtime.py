"""
Integration tests for the mounted MIME type detector through the SDK runtime.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from detect_file_mime_type_mounted import handler


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    target = mount_point / ".__onedata__file_id__file-id"
    target.write_text("content")
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
                    "fileId": "file-id",
                    "name": "document.txt",
                    "type": "REG",
                },
                "metadataKey": "metadata",
            }
        ],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "format": {
                    "fileId": "file-id",
                    "fileName": "document.txt",
                    "mimeType": "text/plain",
                }
            }
        ]
    }
    assert stored == {"metadata.mime-type": b"text/plain"}
