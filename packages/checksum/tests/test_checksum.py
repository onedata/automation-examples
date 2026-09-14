"""Unit tests for the shared checksum core."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
import zlib

import pytest
from onedata_lambda_sdk import JobException

from checksum import (
    AVAILABLE_CHECKSUM_ALGORITHMS,
    assert_supported,
    calculate_checksum,
    is_supported,
    require_supported,
)


def _chunks(data: bytes, size: int = 7) -> list[bytes]:
    return [data[i : i + size] for i in range(0, len(data), size)]


def test_sha256_matches_hashlib() -> None:
    data = b"hello onedata world\n" * 100
    assert calculate_checksum("sha256", _chunks(data)) == hashlib.sha256(data).hexdigest()


def test_adler32_matches_zlib() -> None:
    data = b"adler streamed bytes"
    assert calculate_checksum("adler32", _chunks(data)) == format(zlib.adler32(data), "x")


def test_on_bytes_called_per_chunk() -> None:
    data = b"abc" * 50
    seen: list[int] = []
    got = calculate_checksum("md5", _chunks(data, 10), on_bytes=seen.append)

    assert got == hashlib.md5(data).hexdigest()
    assert sum(seen) == len(data)


def test_shake_has_fixed_length_digest() -> None:
    # shake_* needs an explicit length; the core fixes it (32 bytes -> 64 hex chars).
    assert len(calculate_checksum("shake_128", [b"payload"])) == 64


def test_is_supported() -> None:
    assert is_supported("sha256")
    assert not is_supported("crc32")
    assert "sha512" in AVAILABLE_CHECKSUM_ALGORITHMS


def test_assert_supported_raises_job_exception() -> None:
    assert_supported("sha256")  # supported -> no error
    with pytest.raises(JobException):
        assert_supported("crc32")


def test_require_supported_returns_algorithm_or_raises() -> None:
    assert require_supported("sha256") == "sha256"
    with pytest.raises(JobException):
        require_supported("crc32")
