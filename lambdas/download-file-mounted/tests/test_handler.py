"""
Unit tests for the mounted download handler, exercised through the SDK's in-memory test
helpers. The tests use a temporary Oneclient mount point and stub HTTP/XRootD clients.
"""

__author__ = "Wojciech, Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from download_file_mounted import handler


def test_http_download_writes_file_and_streams_stats(
    mount_point: Path,
    monkeypatch: pytest.MonkeyPatch,
    response_cls: type[Any],
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    chunks = [b"hello ", b"onedata\n"]
    source_url = "https://example.test/file.txt"

    def get(url: str, **kwargs: object) -> object:
        assert url == source_url
        assert kwargs["stream"] is True
        assert kwargs["allow_redirects"] is True
        assert kwargs["headers"] == {"user-agent": handler.USER_AGENT}
        return response_cls(chunks)

    monkeypatch.setattr(handler.requests, "get", get)

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([job_args(source_url, "nested/file.txt", sum(map(len, chunks)))]),
        rc.context,
    )

    assert results == [{"processedFilePath": "nested/file.txt"}]
    assert (mount_point / "nested/file.txt").read_bytes() == b"hello onedata\n"
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 2
    assert ts_names.count("filesProcessed") == 1
    assert rc.heartbeats == 1


def test_existing_file_with_expected_size_is_not_downloaded(
    mount_point: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    target = mount_point / "already/here.txt"
    target.parent.mkdir()
    target.write_bytes(b"cached")

    def get(*args: object, **kwargs: object) -> object:
        raise AssertionError("download should be skipped")

    monkeypatch.setattr(handler.requests, "get", get)

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([job_args("https://example.test/file.txt", "already/here.txt", 6)]),
        rc.context,
    )

    assert results == [{"processedFilePath": "already/here.txt"}]
    assert target.read_bytes() == b"cached"
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("filesProcessed") == 1
    assert "bytesProcessed" not in ts_names
    assert rc.logs["logs"][0]["content"]["message"].startswith("Skipping download")


def test_existing_file_with_wrong_size_is_replaced(
    mount_point: Path,
    monkeypatch: pytest.MonkeyPatch,
    response_cls: type[Any],
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    target = mount_point / "replace/me.txt"
    target.parent.mkdir()
    target.write_bytes(b"old")

    monkeypatch.setattr(
        handler.requests,
        "get",
        lambda *args, **kwargs: response_cls([b"new content"]),
    )

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([job_args("https://example.test/file.txt", "replace/me.txt", 11)]),
        rc.context,
    )

    assert results == [{"processedFilePath": "replace/me.txt"}]
    assert target.read_bytes() == b"new content"
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 1
    assert ts_names.count("filesProcessed") == 1


def test_size_mismatch_is_per_job_exception(
    mount_point: Path,
    monkeypatch: pytest.MonkeyPatch,
    response_cls: type[Any],
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    monkeypatch.setattr(
        handler.requests,
        "get",
        lambda *args, **kwargs: response_cls([b"too short"]),
    )

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([job_args("https://example.test/file.txt", "bad-size.txt", 100)]),
        rc.context,
    )

    assert "exception" in results[0]
    assert "Mismatch between expected" in results[0]["exception"]
    assert (mount_point / "bad-size.txt").read_bytes() == b"too short"
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 1
    assert ts_names.count("filesProcessed") == 1


def test_destination_path_must_stay_within_mount(
    tmp_path: Path,
    mount_point: Path,
    response_cls: type[Any],
    monkeypatch: pytest.MonkeyPatch,
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    monkeypatch.setattr(
        handler.requests,
        "get",
        lambda *args, **kwargs: response_cls([b"outside"]),
    )

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([job_args("https://example.test/file.txt", "../outside.txt", 7)]),
        rc.context,
    )

    assert "exception" in results[0]
    assert "Destination path must stay within" in results[0]["exception"]
    assert not (tmp_path / "outside.txt").exists()
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("filesProcessed") == 1
    assert "bytesProcessed" not in ts_names


def test_xrootd_download_uses_xrootd_client(
    mount_point: Path,
    monkeypatch: pytest.MonkeyPatch,
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    class Status:
        ok = True
        message = ""

    class File:
        def __enter__(self) -> "File":
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def open(self, url: str, flags: object) -> tuple[Status, None]:
            assert url == "root://example.test/file.dat"
            assert flags is sys.modules["XRootD.client.flags"].OpenFlags.READ
            return Status(), None

        def readchunks(self, offset: int, chunksize: int) -> Iterator[bytes]:
            assert offset == 0
            assert chunksize == handler.DOWNLOAD_CHUNK_SIZE
            yield b"xrootd bytes"

    monkeypatch.setattr(sys.modules["XRootD.client"], "File", File)

    rc = build_job_context(config={})
    results = handler.handle(
        build_jobs([job_args("root://example.test/file.dat", "xrootd/file.dat", 12)]),
        rc.context,
    )

    assert results == [{"processedFilePath": "xrootd/file.dat"}]
    assert (mount_point / "xrootd/file.dat").read_bytes() == b"xrootd bytes"
