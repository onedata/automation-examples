"""
Unit tests for the ACL builder handler.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from onedata_lambda_utils.testing import build_job_context, build_jobs

from build_allowing_acl.handler import handle


GROUP_ID = "group-1"
OTHER_GROUP_ID = "group-2"
READ_ACCESS_RIGHT = "READ_OBJECT"
ACCESS_RIGHTS = [READ_ACCESS_RIGHT, "WRITE_OBJECT", "DELETE"]
ACE_MASK = ",".join(ACCESS_RIGHTS)


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
                    "groups": [_group(GROUP_ID), _group(OTHER_GROUP_ID)],
                    "grantedAccessRights": ACCESS_RIGHTS,
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
                    "acemask": ACE_MASK,
                    "identifier": GROUP_ID,
                },
                {
                    "acetype": "ALLOW",
                    "aceflags": "IDENTIFIER_GROUP",
                    "acemask": ACE_MASK,
                    "identifier": OTHER_GROUP_ID,
                },
            ]
        }
    ]
    assert rc.heartbeats == 1


def test_empty_groups_yield_empty_acl() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs([{"groups": [], "grantedAccessRights": [READ_ACCESS_RIGHT]}]),
        rc.context,
    )

    assert results == [{"acl": []}]


def test_empty_access_rights_yield_empty_mask() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs([{"groups": [_group(GROUP_ID)], "grantedAccessRights": []}]),
        rc.context,
    )

    assert results == [
        {
            "acl": [
                {
                    "acetype": "ALLOW",
                    "aceflags": "IDENTIFIER_GROUP",
                    "acemask": "",
                    "identifier": GROUP_ID,
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
                    "groups": [_group(GROUP_ID)],
                    "grantedAccessRights": [READ_ACCESS_RIGHT],
                },
                {
                    "groups": [{}],
                    "grantedAccessRights": [READ_ACCESS_RIGHT],
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
                "acemask": READ_ACCESS_RIGHT,
                "identifier": GROUP_ID,
            }
        ]
    }
    assert "exception" in results[1]
