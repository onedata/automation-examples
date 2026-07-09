"""
Integration tests for the mounted BagIt checksum verifier through the SDK runtime.
"""

__author__ = "Rafał Widziszewski, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from bagit_uploader_calculate_checksum_mounted import handler


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    target = mount_point / ".__onedata__file_id__destination-id" / "file.txt"
    target.parent.mkdir()
    content = b"checksum content"
    target.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    stored: dict[str, bytes] = {"checksum.sha256.expected": f'"{expected}"'.encode()}

    class XAttr:
        def __init__(self, path: Path) -> None:
            assert path == target

        def list(self) -> list[str]:
            return list(stored)

        def get(self, name: str) -> bytes:
            return stored[name]

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler.xattr, "xattr", XAttr)

    request = build_request(
        [{"filePath": ".__onedata__file_id__destination-id/file.txt"}],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "result": {
                    "filePath": str(target),
                    "checksums": {
                        "sha256": {
                            "expected": expected,
                            "calculated": expected,
                            "status": "ok",
                        }
                    },
                }
            }
        ]
    }
    assert stored["checksum.sha256.calculated"] == f'"{expected}"'.encode()
    assert result.streams["stats"][0]["tsName"] == "bytesProcessed_sha256"
