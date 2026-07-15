"""
Integration tests for the mounted download lambda through the real SDK runtime.

They exercise wire parsing, `JobContext` construction, envelope assembly, buffered stream
flushing, and writes to the configured Oneclient mount point.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from download_file_mounted import handler


def test_run_end_to_end_downloads_file_and_flushes_stats(
    mount_point: Path,
    out_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    response_cls: type[Any],
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    chunks = [b"runtime ", b"download\n"]
    source_url = "https://example.test/file.txt"

    def get(url: str, **kwargs: object) -> object:
        assert url == source_url
        assert kwargs["stream"] is True
        assert kwargs["allow_redirects"] is True
        assert kwargs["headers"] == {"user-agent": handler.USER_AGENT}
        return response_cls(chunks)

    monkeypatch.setattr(handler.requests, "get", get)

    request = build_request(
        [job_args(source_url, "runtime/file.txt", sum(map(len, chunks)))],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [{"processedFilePath": "runtime/file.txt"}]}
    assert (mount_point / "runtime/file.txt").read_bytes() == b"runtime download\n"
    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 2
    assert ts_names.count("filesProcessed") == 1


def test_run_returns_per_job_exception_for_failed_download(
    mount_point: Path,
    out_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    response_cls: type[Any],
    job_args: Callable[[str, str, int], dict[str, Any]],
) -> None:
    def get(url: str, **kwargs: object) -> object:
        if url.endswith("/ok.txt"):
            return response_cls([b"ok"])
        return response_cls([b"too short"], status_code=500)

    monkeypatch.setattr(handler.requests, "get", get)

    request = build_request(
        [
            job_args("https://example.test/ok.txt", "ok.txt", 2),
            job_args("https://example.test/bad.txt", "bad.txt", 9),
        ],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    batch = result.envelope["resultsBatch"]
    assert batch[0] == {"processedFilePath": "ok.txt"}
    assert "exception" in batch[1]
    assert "HTTP download failed" in batch[1]["exception"]
    assert (mount_point / "ok.txt").read_bytes() == b"ok"
    assert not (mount_point / "bad.txt").exists()

    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 1
    assert ts_names.count("filesProcessed") == 2
