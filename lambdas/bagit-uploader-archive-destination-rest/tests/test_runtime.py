"""
Integration tests for the REST BagIt archive creator through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from bagit_uploader_archive_destination_rest import handler


class Response:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self) -> dict[str, Any]:
        return self.payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise handler.requests.HTTPError(f"{self.status_code} error")


def test_run_end_to_end(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    archive_states = iter(
        [
            {
                "state": "building",
                "status": "building",
                "stats": {"bytesArchived": 5, "filesArchived": 1},
            },
            {
                "state": "preserved",
                "status": "preserved",
                "stats": {"bytesArchived": 8, "filesArchived": 2},
            },
        ]
    )

    def post(url: str, **kwargs: Any) -> Response:
        if url.endswith("/datasets"):
            return Response({"datasetId": "dataset-id"}, status_code=201)
        if url.endswith("/archives"):
            return Response({"archiveId": "archive-id"}, status_code=201)
        raise AssertionError(url)

    def get(url: str, **kwargs: Any) -> Response:
        assert url.endswith("/archives/archive-id")
        return Response(next(archive_states))

    monkeypatch.setattr(handler.requests, "post", post)
    monkeypatch.setattr(handler.requests, "get", get)
    monkeypatch.setattr(handler.time, "sleep", lambda seconds: None)

    request = build_request(
        [{"destinationDir": {"fileId": "dir-id", "type": "DIR"}}],
        config={},
        oneprovider_domain="provider.test",
        access_token="token",
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [{"archiveId": "archive-id"}]}
    assert [item["tsName"] for item in result.streams["stats"]] == [
        "bytesArchived",
        "filesArchived",
        "bytesArchived",
        "filesArchived",
    ]
