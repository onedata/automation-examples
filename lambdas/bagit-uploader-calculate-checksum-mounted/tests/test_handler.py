"""
Unit tests for the parallel mounted BagIt checksum verifier variant.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
import zlib
from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from bagit_uploader_calculate_checksum_mounted import handler as handler_parallel


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mount = tmp_path / "mnt"
    mount.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount))
    return mount


def test_parallel_variant_verifies_expected_checksums(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = mount_point / ".__onedata__file_id__destination-id" / "file.txt"
    target.parent.mkdir()
    content = b"checksum content"
    target.write_bytes(content)
    expected_sha256 = hashlib.sha256(content).hexdigest()
    expected_adler32 = format(zlib.adler32(content, 1), "x")
    stored: dict[str, bytes] = {
        "checksum.sha256.expected": f'"{expected_sha256}"'.encode(),
        "checksum.adler32.expected": f'"{expected_adler32}"'.encode(),
    }

    class XAttr:
        def __init__(self, path: Path) -> None:
            assert path == target

        def list(self) -> list[str]:
            return list(stored)

        def get(self, name: str) -> bytes:
            return stored[name]

        def set(self, name: str, value: bytes) -> None:
            stored[name] = value

    monkeypatch.setattr(handler_parallel.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler_parallel.handle(
        build_jobs([{"filePath": ".__onedata__file_id__destination-id/file.txt"}]),
        rc.context,
    )

    assert results[0]["result"]["checksums"] == {
        "sha256": {
            "expected": expected_sha256,
            "calculated": expected_sha256,
            "status": "ok",
        },
        "adler32": {
            "expected": expected_adler32,
            "calculated": expected_adler32,
            "status": "ok",
        },
    }
    assert stored["checksum.sha256.calculated"] == f'"{expected_sha256}"'.encode()
    assert stored["checksum.adler32.calculated"] == f'"{expected_adler32}"'.encode()
    assert {m["tsName"] for m in rc.streams["stats"]} == {
        "bytesProcessed_sha256",
        "bytesProcessed_adler32",
    }


def test_parallel_variant_checksum_mismatch_is_per_job_exception(
    mount_point: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = mount_point / ".__onedata__file_id__destination-id" / "file.txt"
    target.parent.mkdir()
    target.write_bytes(b"checksum content")

    class XAttr:
        def __init__(self, path: Path) -> None:
            assert path == target

        def list(self) -> list[str]:
            return ["checksum.sha256.expected"]

        def get(self, name: str) -> bytes:
            return b'"wrong"'

        def set(self, name: str, value: bytes) -> None:
            pass

    monkeypatch.setattr(handler_parallel.xattr, "xattr", XAttr)

    rc = build_job_context(config={})
    results = handler_parallel.handle(
        build_jobs([{"filePath": ".__onedata__file_id__destination-id/file.txt"}]),
        rc.context,
    )

    assert "exception" in results[0]
    assert "Expected file checksum" in results[0]["exception"]
