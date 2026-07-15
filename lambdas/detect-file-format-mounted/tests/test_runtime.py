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


FILE_ID = "file-id"
FILE_NAME = "document.txt"
MISMATCHED_FILE_NAME = "document.bin"
METADATA_KEY = "metadata"
FORMAT_NAME = "ASCII text"
MIME_TYPE = "text/plain"
EXTENSIONS = [".txt"]
EXTENSION_MATCHES = True


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    target = mount_point / f".__onedata__file_id__{FILE_ID}"
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
    }
    assert stored == {
        f"{METADATA_KEY}.format-name": FORMAT_NAME.encode(),
        f"{METADATA_KEY}.mime-type": MIME_TYPE.encode(),
        f"{METADATA_KEY}.is-extension-matching-format": str(EXTENSION_MATCHES).encode(),
    }
