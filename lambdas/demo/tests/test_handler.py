"""
Unit tests for the demo lambda.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

from onedata_lambda_utils.testing import build_job_context, build_jobs

from demo.handler import handle


FILE_ID = "file-id"
FILE_NAME = "example.txt"


def test_greets_input_file() -> None:
    rc = build_job_context(config={})
    jobs = build_jobs(
        [
            {
                "item": {
                    "fileId": FILE_ID,
                    "name": FILE_NAME,
                    "type": "REG",
                }
            }
        ]
    )

    results = handle(jobs, rc.context)

    assert results == [{"result": f"Hello - {FILE_NAME}"}]


def test_handles_multiple_jobs() -> None:
    rc = build_job_context(config={})
    args_batch: list[dict[str, Any]] = [
        {"item": {"fileId": "first-id", "name": "first.txt", "type": "REG"}},
        {"item": {"fileId": "second-id", "name": "second.txt", "type": "REG"}},
    ]

    results = handle(build_jobs(args_batch), rc.context)

    assert results == [
        {"result": "Hello - first.txt"},
        {"result": "Hello - second.txt"},
    ]
