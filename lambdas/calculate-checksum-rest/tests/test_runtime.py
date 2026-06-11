"""
Integration test: drive the real SDK runtime (`run()`) for the REST checksum lambda via
`run_local`, against a local mock HTTPS provider. Exercises wire parsing, `JobContext`
construction (the provider domain / access token come from the wire ctx), the real
`requests` path, the buffered stats flusher, and envelope assembly.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local
from pytest_httpserver import HTTPServer

from calculate_checksum_rest.handler import handle


def test_run_end_to_end(
    httpserver: HTTPServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERIFY_SSL_CERTIFICATES", "false")

    data = b"end to end rest bytes\n" * 800
    httpserver.expect_request("/api/v3/oneprovider/data/f1/content").respond_with_data(data)

    request = build_request(
        [
            {"file": {"fileId": "f1", "type": "REG"}},
            {"file": {"fileId": "d1", "type": "DIR"}},
        ],
        config={"algorithm": "md5", "metadataKey": ""},
        oneprovider_domain=f"{httpserver.host}:{httpserver.port}",
    )
    result = run_local(handle, request, out_dir=tmp_path)

    batch = result.envelope["resultsBatch"]
    assert batch[0]["result"]["checksum"] == hashlib.md5(data).hexdigest()
    assert batch[1]["result"]["checksum"] is None

    # Only the regular file is counted/checksummed; the directory yields null and is neither
    # counted nor requested (matches v2).
    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("filesProcessed") == 1
    assert ts_names.count("bytesProcessed") == 1
