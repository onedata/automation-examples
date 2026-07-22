"""
Unit tests for the REST BagIt archive creator.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from bagit_uploader_archive_destination_rest import handler


ARCHIVE_ID = "archive-id"
DATASET_ID = "dataset-id"
EXISTING_DATASET_ID = "existing-dataset-id"
DESTINATION_DIR_ID = "dir-id"
PROVIDER_DOMAIN = "provider.test"
ACCESS_TOKEN = "token"


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


def test_creates_archive_and_streams_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    posts: list[tuple[str, dict[str, Any] | None]] = []
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
        posts.append((url, kwargs.get("json")))
        if url.endswith("/datasets"):
            return Response({"datasetId": DATASET_ID}, status_code=201)
        if url.endswith("/archives"):
            return Response({"archiveId": ARCHIVE_ID}, status_code=201)
        raise AssertionError(url)

    def get(url: str, **kwargs: Any) -> Response:
        assert url.endswith(f"/archives/{ARCHIVE_ID}")
        return Response(next(archive_states))

    monkeypatch.setattr(handler.requests, "post", post)
    monkeypatch.setattr(handler.requests, "get", get)
    monkeypatch.setattr(handler.time, "sleep", lambda seconds: None)

    rc = build_job_context(
        config={},
        oneprovider_domain=PROVIDER_DOMAIN,
        access_token=ACCESS_TOKEN,
    )
    results = handler.handle(
        build_jobs([{"destinationDir": {"fileId": DESTINATION_DIR_ID, "type": "DIR"}}]),
        rc.context,
    )

    assert results == [{"archiveId": ARCHIVE_ID}]
    assert posts[0][1] == {"rootFileId": DESTINATION_DIR_ID, "protectionFlags": []}
    assert posts[1][1]["config"] == {"includeDip": True, "layout": "bagit"}
    ts_names = [item["tsName"] for item in rc.streams["stats"]]
    assert ts_names == ["bytesArchived", "filesArchived", "bytesArchived", "filesArchived"]


def test_existing_dataset_is_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    def post(url: str, **kwargs: Any) -> Response:
        if url.endswith("/datasets"):
            return Response({}, status_code=409)
        if url.endswith("/archives"):
            return Response({"archiveId": ARCHIVE_ID}, status_code=201)
        raise AssertionError(url)

    def get(url: str, **kwargs: Any) -> Response:
        if url.endswith(f"/data/{DESTINATION_DIR_ID}/dataset/summary"):
            return Response({"directDataset": EXISTING_DATASET_ID})
        if url.endswith(f"/archives/{ARCHIVE_ID}"):
            return Response(
                {
                    "state": "preserved",
                    "status": "preserved",
                    "stats": {"bytesArchived": 0, "filesArchived": 0},
                }
            )
        raise AssertionError(url)

    monkeypatch.setattr(handler.requests, "post", post)
    monkeypatch.setattr(handler.requests, "get", get)

    rc = build_job_context(
        config={},
        oneprovider_domain=PROVIDER_DOMAIN,
        access_token=ACCESS_TOKEN,
    )
    results = handler.handle(
        build_jobs([{"destinationDir": {"fileId": DESTINATION_DIR_ID, "type": "DIR"}}]),
        rc.context,
    )

    assert results == [{"archiveId": ARCHIVE_ID}]
    assert rc.logs["logs"][0]["content"]["message"] == "Dataset already established."
