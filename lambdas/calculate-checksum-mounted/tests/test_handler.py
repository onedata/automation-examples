"""
Unit tests for the v3 checksum lambda, exercised through the SDK's in-memory test helpers
(`onedata_lambda_utils.testing`) -- no Docker, no provider, no real `/out`.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
import zlib
from pathlib import Path

import pytest
from onedata_lambda_utils import JobException
from onedata_lambda_utils.testing import build_job_context, build_jobs

from calculate_checksum_mounted.handler import handle


def _seed_file(mount_point: Path, file_id: str, data: bytes) -> None:
    (mount_point / f".__onedata__file_id__{file_id}").write_bytes(data)


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mp = tmp_path / "mnt"
    mp.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mp))
    return mp


def test_sha256_checksum(mount_point: Path) -> None:
    data = b"hello onedata\n" * 1000
    _seed_file(mount_point, "f1", data)

    rc = build_job_context(config={"algorithm": "sha256", "metadataKey": ""})
    results = handle(build_jobs([{"file": {"fileId": "f1"}}]), rc.context)

    assert results == [
        {
            "result": {
                "fileId": "f1",
                "algorithm": "sha256",
                "checksum": hashlib.sha256(data).hexdigest(),
            }
        }
    ]
    # One BytesProcessed (single chunk) + one FilesProcessed measurement on the stats stream.
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("filesProcessed") == 1
    assert ts_names.count("bytesProcessed") == 1
    # @per_job heartbeats once per completed job.
    assert rc.heartbeats >= 1


def test_adler32_checksum(mount_point: Path) -> None:
    data = b"some bytes for adler" * 10
    _seed_file(mount_point, "f2", data)

    rc = build_job_context(config={"algorithm": "adler32", "metadataKey": ""})
    results = handle(build_jobs([{"file": {"fileId": "f2"}}]), rc.context)

    assert results[0]["result"]["checksum"] == format(zlib.adler32(data), "x")


def test_missing_file_yields_none_checksum(mount_point: Path) -> None:
    rc = build_job_context(config={"algorithm": "md5", "metadataKey": ""})
    results = handle(build_jobs([{"file": {"fileId": "does-not-exist"}}]), rc.context)

    assert results[0]["result"]["checksum"] is None
    # A missing/non-regular file is not checksummed, so nothing is streamed for it -- no
    # bytesProcessed and (matching v2) no filesProcessed.
    assert rc.streams["stats"] == []


def test_unsupported_algorithm_fails_batch(mount_point: Path) -> None:
    _seed_file(mount_point, "f3", b"x")

    rc = build_job_context(config={"algorithm": "crc32", "metadataKey": ""})
    # The algorithm is validated once per batch (the precondition), so an unsupported one
    # fails the whole batch with a JobException -- which run() turns into a clean top-level
    # {"exception": ...} -- rather than a per-job AtmException repeated across every job.
    with pytest.raises(JobException):
        handle(build_jobs([{"file": {"fileId": "f3"}}]), rc.context)


def test_batch_isolates_per_job_failures(mount_point: Path) -> None:
    _seed_file(mount_point, "ok", b"good content")

    rc = build_job_context(config={"algorithm": "md5", "metadataKey": ""})
    # Second job's args are malformed (no "fileId") -> KeyError inside the handler ->
    # AtmException for that job only; the first job still succeeds.
    results = handle(
        build_jobs([{"file": {"fileId": "ok"}}, {"file": {}}]),
        rc.context,
    )

    assert results[0]["result"]["checksum"] == hashlib.md5(b"good content").hexdigest()
    assert "exception" in results[1]


def test_xattr_metadata_written(mount_point: Path) -> None:
    import xattr

    data = b"checksum me please"
    target = mount_point / ".__onedata__file_id__fx"
    target.write_bytes(data)

    # The local temp filesystem may not support user extended attributes; skip if so.
    try:
        xattr.xattr(str(target)).set("user.probe", b"1")
    except OSError:
        pytest.skip("filesystem does not support extended attributes")

    rc = build_job_context(config={"algorithm": "md5", "metadataKey": "user.onedata.checksum"})
    results = handle(build_jobs([{"file": {"fileId": "fx"}}]), rc.context)

    expected = hashlib.md5(data).hexdigest()
    assert results[0]["result"]["checksum"] == expected
    assert xattr.xattr(str(target)).get("user.onedata.checksum") == expected.encode()
