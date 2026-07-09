"""
Integration tests for the REST directory-structure lambda through the real SDK runtime.

They exercise wire parsing, `JobContext` construction, envelope assembly, and real REST
requests against a local mock HTTPS provider.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local
from pytest_httpserver import HTTPServer

from create_dir_structure_rest.handler import handle


def _job_args(parent_id: str, parent_path: str, dir_paths: list[str]) -> dict[str, object]:
    return {
        "targetDir": {
            "fileId": parent_id,
            "path": parent_path,
        },
        "dirPaths": dir_paths,
    }


def test_run_end_to_end_creates_directories(
    httpserver: HTTPServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERIFY_SSL_CERTIFICATES", "false")

    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/alpha",
        method="PUT",
        query_string="type=DIR&create_parents=true",
        headers={"x-auth-token": "test-token"},
    ).respond_with_json({"fileId": "alpha-id"}, status=201)
    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/beta/gamma",
        method="PUT",
        query_string="type=DIR&create_parents=true",
        headers={"x-auth-token": "test-token"},
    ).respond_with_json({"fileId": "gamma-id"}, status=201)

    request = build_request(
        [_job_args("parent-id", "space/root", ["alpha", "beta/gamma"])],
        config={},
        oneprovider_domain=f"{httpserver.host}:{httpserver.port}",
        access_token="test-token",
    )
    result = run_local(handle, request, out_dir=tmp_path)

    assert result.envelope == {
        "resultsBatch": [{"directories": [{"fileId": "alpha-id"}, {"fileId": "gamma-id"}]}]
    }


def test_run_returns_per_job_exception_without_stopping_batch(
    httpserver: HTTPServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERIFY_SSL_CERTIFICATES", "false")

    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/existing",
        method="PUT",
        query_string="type=DIR&create_parents=true",
        headers={"x-auth-token": "test-token"},
    ).respond_with_json(
        {"error": {"details": {"errno": "eexist"}}},
        status=400,
    )
    httpserver.expect_request(
        "/api/v3/oneprovider/lookup-file-id/space/root/existing",
        method="POST",
        headers={"x-auth-token": "test-token"},
    ).respond_with_json({"fileId": "existing-id"})
    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/not-dir",
        method="PUT",
        query_string="type=DIR&create_parents=true",
        headers={"x-auth-token": "test-token"},
    ).respond_with_json(
        {"error": {"details": {"errno": "enotdir"}}},
        status=400,
    )

    request = build_request(
        [
            _job_args("parent-id", "space/root", ["existing"]),
            _job_args("parent-id", "space/root", ["not-dir"]),
        ],
        config={},
        oneprovider_domain=f"{httpserver.host}:{httpserver.port}",
        access_token="test-token",
    )
    result = run_local(handle, request, out_dir=tmp_path)

    batch = result.envelope["resultsBatch"]
    assert batch[0] == {"directories": [{"fileId": "existing-id"}]}
    assert "exception" in batch[1]
    assert '"not-dir" path already exists and is not a directory' in batch[1]["exception"]
