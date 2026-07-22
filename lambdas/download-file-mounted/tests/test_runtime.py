"""
Integration tests for the mounted download lambda through the real SDK runtime.

They exercise wire parsing, `JobContext` construction, envelope assembly, buffered stream
flushing, and writes to the configured Oneclient mount point.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from download_file_mounted import handler

from .conftest import Response, job_args


SOURCE_URL = "https://example.test/file.txt"
RUNTIME_DESTINATION_PATH = "runtime/file.txt"
RUNTIME_CONTENT = b"runtime download\n"
OK_SOURCE_URL = "https://example.test/ok.txt"
BAD_SOURCE_URL = "https://example.test/bad.txt"
OK_DESTINATION_PATH = "ok.txt"
BAD_DESTINATION_PATH = "bad.txt"
OK_CONTENT = b"ok"


def test_run_end_to_end_downloads_file_and_flushes_stats(
    mount_point: Path,
    out_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunks = [b"runtime ", b"download\n"]

    def get(url: str, **kwargs: object) -> object:
        assert url == SOURCE_URL
        assert kwargs["stream"] is True
        assert kwargs["allow_redirects"] is True
        assert kwargs["headers"] == {"user-agent": handler.USER_AGENT}
        return Response(chunks)

    monkeypatch.setattr(handler.requests, "get", get)

    request = build_request(
        [job_args(SOURCE_URL, RUNTIME_DESTINATION_PATH, sum(map(len, chunks)))],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [{"processedFilePath": RUNTIME_DESTINATION_PATH}]}
    assert (mount_point / RUNTIME_DESTINATION_PATH).read_bytes() == RUNTIME_CONTENT
    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 2
    assert ts_names.count("filesProcessed") == 1


def test_run_returns_per_job_exception_for_failed_download(
    mount_point: Path,
    out_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def get(url: str, **kwargs: object) -> object:
        if url == OK_SOURCE_URL:
            return Response([OK_CONTENT])
        return Response([b"too short"], status_code=500)

    monkeypatch.setattr(handler.requests, "get", get)

    request = build_request(
        [
            job_args(OK_SOURCE_URL, OK_DESTINATION_PATH, len(OK_CONTENT)),
            job_args(BAD_SOURCE_URL, BAD_DESTINATION_PATH, 9),
        ],
        config={},
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    batch = result.envelope["resultsBatch"]
    assert batch[0] == {"processedFilePath": OK_DESTINATION_PATH}
    assert "exception" in batch[1]
    assert "HTTP download failed" in batch[1]["exception"]
    assert (mount_point / OK_DESTINATION_PATH).read_bytes() == OK_CONTENT
    assert not (mount_point / BAD_DESTINATION_PATH).exists()

    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("bytesProcessed") == 1
    assert ts_names.count("filesProcessed") == 2
