"""
Integration tests for the mounted file format detector through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from detect_file_format_mounted import handler


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    target = mount_point / ".__onedata__file_id__file-id"
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
    }
    assert stored == {
        "metadata.format-name": b"ASCII text",
        "metadata.mime-type": b"text/plain",
        "metadata.is-extension-matching-format": b"True",
    }
