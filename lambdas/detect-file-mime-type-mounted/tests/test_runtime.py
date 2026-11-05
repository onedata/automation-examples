"""
Integration tests for the mounted MIME type detector through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_sdk.testing import build_request, run_local

from detect_file_mime_type_mounted import handler


FILE_ID = "file-id"
FILE_NAME = "document.txt"
UNKNOWN_FILE_NAME = "unknown.extension-not-known"
METADATA_KEY = "metadata"
MIME_TYPE = "text/plain"
UNKNOWN_MIME_TYPE = "unknown"


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    target = mount_point / f".__onedata__file_id__{FILE_ID}"
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
                    "fileId": FILE_ID,
                    "name": FILE_NAME,
                    "type": "REG",
                },
                "metadataKey": METADATA_KEY,
            }
        ],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "format": {
                    "fileId": FILE_ID,
                    "fileName": FILE_NAME,
                    "mimeType": MIME_TYPE,
                }
            }
        ]
    }
    assert stored == {f"{METADATA_KEY}.mime-type": MIME_TYPE.encode()}
