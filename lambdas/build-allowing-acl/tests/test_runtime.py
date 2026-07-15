"""
Integration tests for the ACL builder lambda through the real SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

from onedata_lambda_utils.testing import build_request, run_local

from build_allowing_acl.handler import handle


def _group(group_id: str) -> dict[str, str]:
    return {
        "groupId": group_id,
        "name": group_id,
        "type": "team",
    }


def test_run_end_to_end(tmp_path: Path) -> None:
    request = build_request(
        [
            {
                "groups": [_group("group-1"), _group("group-2")],
                "grantedAccessRights": ["READ_OBJECT", "WRITE_OBJECT"],
            }
        ],
        config={},
    )

    result = run_local(handle, request, out_dir=tmp_path)

    assert result.envelope == {
        "resultsBatch": [
            {
                "acl": [
                    {
                        "acetype": "ALLOW",
                        "aceflags": "IDENTIFIER_GROUP",
                        "acemask": "READ_OBJECT,WRITE_OBJECT",
                        "identifier": "group-1",
                    },
                    {
                        "acetype": "ALLOW",
                        "aceflags": "IDENTIFIER_GROUP",
                        "acemask": "READ_OBJECT,WRITE_OBJECT",
                        "identifier": "group-2",
                    },
                ]
            }
        ]
    }


def test_run_returns_per_job_exception_for_malformed_job(tmp_path: Path) -> None:
    request = build_request(
        [
            {
                "groups": [_group("group-1")],
                "grantedAccessRights": ["READ_OBJECT"],
            },
            {
                "groups": [{}],
                "grantedAccessRights": ["READ_OBJECT"],
            },
        ],
        config={},
    )

    result = run_local(handle, request, out_dir=tmp_path)

    batch = result.envelope["resultsBatch"]
    assert batch[0] == {
        "acl": [
            {
                "acetype": "ALLOW",
                "aceflags": "IDENTIFIER_GROUP",
                "acemask": "READ_OBJECT",
                "identifier": "group-1",
            }
        ]
    }
    assert "exception" in batch[1]
