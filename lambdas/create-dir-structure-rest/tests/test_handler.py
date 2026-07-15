"""
Unit tests for the REST directory-structure handler.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs
from pytest_httpserver import HTTPServer

from create_dir_structure_rest import handler


@pytest.fixture(autouse=True)
def _disable_ssl_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERIFY_SSL_CERTIFICATES", "false")


def _domain(httpserver: HTTPServer) -> str:
    return f"{httpserver.host}:{httpserver.port}"


def _context(httpserver: HTTPServer):
    return build_job_context(
        config={},
        oneprovider_domain=_domain(httpserver),
        access_token="test-token",
    )


def _job_args(dir_paths: list[str]) -> dict[str, object]:
    return {
        "targetDir": {
            "fileId": "parent-id",
            "path": "space/root",
        },
        "dirPaths": dir_paths,
    }


def test_creates_all_directories(httpserver: HTTPServer) -> None:
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

    rc = _context(httpserver)
    results = handler.handle(build_jobs([_job_args(["alpha", "beta/gamma"])]), rc.context)

    assert results == [{"directories": [{"fileId": "alpha-id"}, {"fileId": "gamma-id"}]}]


def test_existing_directory_is_resolved_by_lookup(httpserver: HTTPServer) -> None:
    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/existing",
        method="PUT",
        query_string="type=DIR&create_parents=true",
    ).respond_with_json(
        {"error": {"details": {"errno": "eexist"}}},
        status=400,
    )
    httpserver.expect_request(
        "/api/v3/oneprovider/lookup-file-id/space/root/existing",
        method="POST",
        headers={"x-auth-token": "test-token"},
    ).respond_with_json({"fileId": "existing-id"})

    rc = _context(httpserver)
    results = handler.handle(build_jobs([_job_args(["existing"])]), rc.context)

    assert results == [{"directories": [{"fileId": "existing-id"}]}]


def test_enotdir_is_per_job_exception(httpserver: HTTPServer) -> None:
    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/not-dir",
        method="PUT",
        query_string="type=DIR&create_parents=true",
    ).respond_with_json(
        {"error": {"details": {"errno": "enotdir"}}},
        status=400,
    )

    rc = _context(httpserver)
    results = handler.handle(build_jobs([_job_args(["not-dir"])]), rc.context)

    assert "exception" in results[0]
    assert '"not-dir" path already exists and is not a directory' in results[0]["exception"]


def test_rest_failure_is_per_job_exception(httpserver: HTTPServer) -> None:
    httpserver.expect_request(
        "/api/v3/oneprovider/data/parent-id/path/broken",
        method="PUT",
        query_string="type=DIR&create_parents=true",
    ).respond_with_data("nope", status=500)

    rc = _context(httpserver)
    results = handler.handle(build_jobs([_job_args(["broken"])]), rc.context)

    assert "exception" in results[0]


def test_url_builders_encode_paths() -> None:
    assert (
        handler._build_create_dir_rest_url("oneprovider.test", "parent-id", "a b/c?d#e")
        == "https://oneprovider.test/api/v3/oneprovider/data/parent-id/path/a%20b/c%3Fd%23e"
    )
    assert (
        handler._build_get_file_id_rest_url(
            "oneprovider.test",
            "/space/root",
            "/a b/c?d#e",
        )
        == "https://oneprovider.test/api/v3/oneprovider/lookup-file-id//space/root/a%20b/c%3Fd%23e"
    )
