"""
Integration tests for the REST ACL setter through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from set_acl_rest import handler


class Response:
    def raise_for_status(self) -> None:
        pass


def test_run_end_to_end(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    puts: list[tuple[str, dict[str, Any]]] = []

    def put(url: str, **kwargs: Any) -> Response:
        puts.append((url, kwargs))
        return Response()

    monkeypatch.setattr(handler.requests, "put", put)

    request = build_request(
        [
            {
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
        ],
        config={},
        oneprovider_domain="provider.test",
        access_token="token",
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [None]}
    assert puts[0][0] == ("https://provider.test/api/v3/oneprovider/data/file-id/metadata/xattrs")
    assert puts[0][1]["json"] == {
        "cdmi_acl": [
            {
                "acetype": "ALLOW",
                "aceflags": "IDENTIFIER_GROUP",
                "acemask": "READ_OBJECT",
                "identifier": "group-id",
            }
        ]
    }
