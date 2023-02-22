"""
A lambda which returns its input as output.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023 ACK CYFRONET AGH"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"


import json
import random
import time

from onedata_lambda_utils.types import (
    AtmException,
    AtmHeartbeatCallback,
    AtmJobBatchRequest,
    AtmJobBatchResponse,
    AtmObject,
)
from typing_extensions import TypeAlias, TypedDict

##===================================================================
## Lambda interface
##===================================================================


class TaskConfig(TypedDict):
    sleepDurationSec: float
    exceptionProbability: int
    streamResults: bool


JobArgs: TypeAlias = AtmObject


##===================================================================
## Lambda implementation
##===================================================================


def handle(
    job_batch_request: AtmJobBatchRequest[JobArgs, TaskConfig],
    heartbeat_callback: AtmHeartbeatCallback,
) -> AtmJobBatchResponse[JobArgs]:
    task_config = job_batch_request["ctx"]["config"]

    if sleep_duration := task_config["sleepDurationSec"]:
        sleep_until = time.time() + sleep_duration

        while time.time() < sleep_until:
            heartbeat_callback()
            time.sleep(0.1)

    results = []
    for job_args in job_batch_request["argsBatch"]:
        if random.randint(1, 100) <= task_config["exceptionProbability"]:
            results.append(AtmException(exception="Random failure"))

        elif task_config["streamResults"]:
            results.append(None)

            for arg_name, arg_value in job_args.values():
                with open(f"/out/{arg_name}", "a+") as f:
                    json.dump(arg_value, f)
                    f.write("\n")

        else:
            results.append(job_args)

    return {"resultsBatch": results}
