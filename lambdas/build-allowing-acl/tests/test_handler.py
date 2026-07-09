"""
Unit tests for the ACL builder handler.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from onedata_lambda_utils.testing import build_job_context, build_jobs

from build_allowing_acl.handler import handle


def _group(group_id: str) -> dict[str, str]:
    return {
        "groupId": group_id,
        "name": group_id,
        "type": "team",
    }


def test_builds_allowing_acl_for_groups() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs(
            [
                {
                    "groups": [_group("group-1"), _group("group-2")],
                    "grantedAccessRights": ["READ_OBJECT", "WRITE_OBJECT", "DELETE"],
                }
            ]
        ),
        rc.context,
    )

    assert results == [
        {
            "acl": [
                {
                    "acetype": "ALLOW",
                    "aceflags": "IDENTIFIER_GROUP",
                    "acemask": "READ_OBJECT,WRITE_OBJECT,DELETE",
                    "identifier": "group-1",
                },
                {
                    "acetype": "ALLOW",
                    "aceflags": "IDENTIFIER_GROUP",
                    "acemask": "READ_OBJECT,WRITE_OBJECT,DELETE",
                    "identifier": "group-2",
                },
            ]
        }
    ]
    assert rc.heartbeats == 1


def test_empty_groups_yield_empty_acl() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs([{"groups": [], "grantedAccessRights": ["READ_OBJECT"]}]),
        rc.context,
    )

    assert results == [{"acl": []}]


def test_empty_access_rights_yield_empty_mask() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs([{"groups": [_group("group-1")], "grantedAccessRights": []}]),
        rc.context,
    )

    assert results == [
        {
            "acl": [
                {
                    "acetype": "ALLOW",
                    "aceflags": "IDENTIFIER_GROUP",
                    "acemask": "",
                    "identifier": "group-1",
                }
            ]
        }
    ]


def test_batch_isolates_malformed_jobs() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs(
            [
                {
                    "groups": [_group("group-1")],
                    "grantedAccessRights": ["READ_OBJECT"],
                },
                {
                    "groups": [{}],
                    "grantedAccessRights": ["READ_OBJECT"],
                },
            ]
        ),
        rc.context,
    )

    assert results[0] == {
        "acl": [
            {
                "acetype": "ALLOW",
                "aceflags": "IDENTIFIER_GROUP",
                "acemask": "READ_OBJECT",
                "identifier": "group-1",
            }
        ]
    }
    assert "exception" in results[1]
