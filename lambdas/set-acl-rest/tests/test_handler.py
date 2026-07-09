"""
Unit tests for the REST ACL setter.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from set_acl_rest import handler


class Response:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise handler.requests.HTTPError(f"{self.status_code} error")


def _job_args() -> dict[str, Any]:
    return {
        "targetFile": {
            "fileId": "file-id",
            "name": "target",
            "type": "REG",
        },
        "acl": [
            {
                "acetype": "ALLOW",
                "aceflags": "IDENTIFIER_GROUP",
                "acemask": "READ_OBJECT",
                "identifier": "group-id",
            }
        ],
    }


def test_sets_acl(monkeypatch: pytest.MonkeyPatch) -> None:
    puts: list[tuple[str, dict[str, Any]]] = []

    def put(url: str, **kwargs: Any) -> Response:
        puts.append((url, kwargs))
        return Response()

    monkeypatch.setattr(handler.requests, "put", put)

    rc = build_job_context(
        config={},
        oneprovider_domain="provider.test",
        access_token="token",
    )
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert results == [None]
    assert puts[0][0] == ("https://provider.test/api/v3/oneprovider/data/file-id/metadata/xattrs")
    assert puts[0][1]["headers"] == {
        "x-auth-token": "token",
        "content-type": "application/json",
    }
    assert puts[0][1]["json"] == {"cdmi_acl": _job_args()["acl"]}
    assert puts[0][1]["timeout"] == handler.REST_REQUEST_TIMEOUT


def test_rest_error_is_per_job_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def put(url: str, **kwargs: Any) -> Response:
        return Response(status_code=500)

    monkeypatch.setattr(handler.requests, "put", put)

    rc = build_job_context(
        config={},
        oneprovider_domain="provider.test",
        access_token="token",
    )
    results = handler.handle(build_jobs([_job_args()]), rc.context)

    assert "exception" in results[0]
    assert "REST request failed" in results[0]["exception"]


def test_verify_ssl_env_is_read_at_call_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERIFY_SSL_CERTIFICATES", "false")

    assert handler._verify_ssl() is False
