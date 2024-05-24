"""
A lambda that creates acl object using given list of groups or users and mask.
Acl object can be passed to lambda set acl which sets acl to the given file.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


from typing import List, Union

from typing_extensions import TypedDict

from onedata_lambda_utils.types import (
    AtmException,
    AtmGroup,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)

##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    groups: List[AtmGroup]
    # users: TODO VFS-12008 implement section responsible for building acl granting permissions to users
    # list of flags used to build the ACE mask, eg. ["ADD_OBJECT", "READ_OBJECT", "DELETE"]
    grantedAccessRights: List[str]


class JobResults(TypedDict):
    acl: List[AtmObject]


##===================================================================
## Lambda implementation
##===================================================================


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
    _heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobResults]:

    results = [run_job(job_args) for job_args in job_batch_request["argsBatch"]]
    return {"resultsBatch": results}


def run_job(job_args: JobArgs) -> Union[JobResults, AtmException]:
    ace_common = {
        "acetype": "ALLOW",
        "aceflags": "IDENTIFIER_GROUP",
        "acemask": ",".join(job_args["grantedAccessRights"]),
    }
    acl = [
        {**ace_common, "identifier": group["groupId"]} for group in job_args["groups"]
    ]
    return {"acl": acl}
