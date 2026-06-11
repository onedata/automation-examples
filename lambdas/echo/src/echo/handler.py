"""
A lambda created mainly for testing the automation workflows mechanism.
Returns its input as output and optionally throws exceptions and/or delays execution.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import random
import time
from typing import NotRequired, TypedDict

from onedata_lambda_utils import (
    AtmException,
    AtmObject,
    Job,
    JobContext,
)


##===================================================================
## Lambda interface
##===================================================================


class TaskConfig(TypedDict):
    sleepDurationSec: float
    exceptionProbability: float  # range: [0, 1]
    streamResults: bool
    wrapResultInArray: NotRequired[bool | None]


type JobArgs = AtmObject

type JobResult = AtmObject | AtmException | None


##===================================================================
## Lambda implementation
##===================================================================


def handle(jobs: list[Job[JobArgs]], ctx: JobContext[TaskConfig]) -> list[JobResult]:
    config = ctx.config
    _sleep_with_heartbeat(config["sleepDurationSec"], ctx)

    results: list[JobResult] = []
    for job in jobs:
        args = job.args
        if config.get("wrapResultInArray"):
            args = {name: [value] for name, value in args.items()}

        if random.random() <= config["exceptionProbability"]:
            results.append(AtmException(exception="Random exception"))
        elif config["streamResults"]:
            results.append(None)
            for name, value in args.items():
                ctx.result_streamer(name).stream_item(value)
        else:
            results.append(args)

    return results


def _sleep_with_heartbeat(duration_sec: float, ctx: JobContext[TaskConfig]) -> None:
    if not duration_sec:
        return

    sleep_until = time.time() + duration_sec
    while time.time() < sleep_until:
        ctx.heartbeat()
        time.sleep(0.1)
