"""
Unit tests for the REST checksum handler: call `handle(jobs, ctx)` directly (fake context),
with the provider domain pointed at a local mock HTTPS server.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib

import pytest
from onedata_lambda_sdk.testing import build_job_context, build_jobs
from pytest_httpserver import HTTPServer

from calculate_checksum_rest.handler import handle


@pytest.fixture(autouse=True)
def _disable_ssl_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERIFY_SSL_CERTIFICATES", "false")


def _domain(httpserver: HTTPServer) -> str:
    return f"{httpserver.host}:{httpserver.port}"


def _context(httpserver: HTTPServer, **config: object):
    return build_job_context(
        config={"algorithm": "sha256", "metadataKey": "", **config},
        oneprovider_domain=_domain(httpserver),
        access_token="test-token",
    )


def test_sha256_over_rest(httpserver: HTTPServer) -> None:
    data = b"rest checksum content\n" * 500
    httpserver.expect_request("/api/v3/oneprovider/data/f1/content").respond_with_data(data)

    rc = _context(httpserver)
    results = handle(build_jobs([{"file": {"fileId": "f1", "type": "REG"}}]), rc.context)

    assert results[0]["result"]["checksum"] == hashlib.sha256(data).hexdigest()
    ts_names = [m["tsName"] for m in rc.streams["stats"]]
    assert ts_names.count("filesProcessed") == 1
    assert ts_names.count("bytesProcessed") == 1


def test_non_regular_file_yields_none_and_makes_no_request(httpserver: HTTPServer) -> None:
    # No expectation registered -> any HTTP call would fail the test.
    rc = _context(httpserver)
    results = handle(build_jobs([{"file": {"fileId": "d1", "type": "DIR"}}]), rc.context)

    assert results[0]["result"]["checksum"] is None


def test_metadata_key_puts_xattr(httpserver: HTTPServer) -> None:
    data = b"store my checksum"
    httpserver.expect_request("/api/v3/oneprovider/data/f2/content").respond_with_data(data)
    httpserver.expect_request(
        "/api/v3/oneprovider/data/f2/metadata/xattrs", method="PUT"
    ).respond_with_data("", status=204)

    rc = _context(httpserver, metadataKey="checksum.sha256")
    results = handle(build_jobs([{"file": {"fileId": "f2", "type": "REG"}}]), rc.context)

    expected = hashlib.sha256(data).hexdigest()
    assert results[0]["result"]["checksum"] == expected
    # The xattr PUT carried the computed checksum under the configured key.
    put = next(r for r, _ in httpserver.log if r.method == "PUT")
    assert put.get_json() == {"checksum.sha256": expected}


def test_http_error_is_per_job_exception(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/api/v3/oneprovider/data/bad/content").respond_with_data(
        "nope", status=500
    )

    rc = _context(httpserver)
    results = handle(build_jobs([{"file": {"fileId": "bad", "type": "REG"}}]), rc.context)

    assert "exception" in results[0]
