"""
Integration test: drive the real SDK runtime (`run()`) end to end via `run_local`.

Unlike `test_handler.py` (handler logic on fake context), this goes through wire parsing,
`JobContext` construction, the buffered stats flusher writing files, and envelope
assembly -- asserting on both the response envelope and what landed on `/out/stats`.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from calculate_checksum_mounted.handler import handle


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))

    data = b"integration checksum bytes\n" * 1000
    (mount_point / ".__onedata__file_id__f1").write_bytes(data)

    request = build_request(
        [{"file": {"fileId": "f1"}}, {"file": {"fileId": "missing"}}],
        config={"algorithm": "sha256", "metadataKey": ""},
    )
    result = run_local(handle, request, out_dir=tmp_path)

    batch = result.envelope["resultsBatch"]
    assert batch[0]["result"]["checksum"] == hashlib.sha256(data).hexdigest()
    assert batch[1]["result"]["checksum"] is None

    # Only the regular file is counted/checksummed; the missing one yields null and is
    # neither counted nor read (matches v2).
    ts_names = [m["tsName"] for m in result.streams["stats"]]
    assert ts_names.count("filesProcessed") == 1
    assert ts_names.count("bytesProcessed") == 1
