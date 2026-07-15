"""
Integration tests for the REST ACL setter through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from set_acl_rest import handler


FILE_ID = "file-id"
GROUP_ID = "group-id"
TARGET_NAME = "target"
ACE_TYPE = "ALLOW"
ACE_FLAGS = "IDENTIFIER_GROUP"
ACE_MASK = "READ_OBJECT"
PROVIDER_DOMAIN = "provider.test"
ACCESS_TOKEN = "token"


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
                    "fileId": FILE_ID,
                    "name": TARGET_NAME,
                    "type": "REG",
                },
                "acl": [
                    {
                        "acetype": ACE_TYPE,
                        "aceflags": ACE_FLAGS,
                        "acemask": ACE_MASK,
                        "identifier": GROUP_ID,
                    }
                ],
            }
        ],
        config={},
        oneprovider_domain=PROVIDER_DOMAIN,
        access_token=ACCESS_TOKEN,
    )
    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [None]}
    assert puts[0][0] == (f"https://{PROVIDER_DOMAIN}/api/v3/oneprovider/data/{FILE_ID}/metadata/xattrs")
    assert puts[0][1]["json"] == {
        "cdmi_acl": [
            {
                "acetype": ACE_TYPE,
                "aceflags": ACE_FLAGS,
                "acemask": ACE_MASK,
                "identifier": GROUP_ID,
            }
        ]
    }
