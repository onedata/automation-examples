"""
A lambda that creates acl object using given list of groups or users and mask.
Acl object can be passed to lambda set acl which sets acl to the given file.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


from typing import TypedDict

from onedata_lambda_sdk import Job, JobContext, per_job
from onedata_lambda_sdk.types import (
    AtmGroup,
    AtmObject,
)


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    groups: list[AtmGroup]
    # users: TODO VFS-12008 implement section responsible for building
    #  acl granting permissions to users
    # list of flags used to build the ACE mask, eg.
    # ["ADD_OBJECT", "READ_OBJECT", "DELETE"]
    grantedAccessRights: list[str]


class JobResult(TypedDict):
    acl: list[AtmObject]


##===================================================================
## Lambda implementation
##===================================================================


@per_job
def handle(job: Job[JobArgs], _ctx: JobContext[AtmObject]) -> JobResult:

    groups = job.args["groups"]
    granted_access_rights = job.args["grantedAccessRights"]

    ace_common = {
        "acetype": "ALLOW",
        "aceflags": "IDENTIFIER_GROUP",
        "acemask": ",".join(granted_access_rights),
    }
    acl = [{**ace_common, "identifier": group["groupId"]} for group in groups]
    return {"acl": acl}
