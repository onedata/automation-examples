"""
Unit tests for the placeholder substitution lambda.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2024-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from typing import Any

from onedata_lambda_utils.testing import build_job_context, build_jobs

from substitute_placeholders.handler import handle


def _job_args(template: str, mappings: dict[str, str]) -> dict[str, Any]:
    return {
        "template": template,
        "mappings": mappings,
    }


def test_substitutes_placeholders() -> None:
    rc = build_job_context(config={})

    results = handle(
        build_jobs(
            [
                _job_args(
                    "Hello $name, file: ${file_id}",
                    {"name": "Alice", "file_id": "file-id"},
                )
            ]
        ),
        rc.context,
    )

    assert results == [{"output": "Hello Alice, file: file-id"}]


def test_missing_mapping_is_per_job_exception() -> None:
    rc = build_job_context(config={})

    results = handle(build_jobs([_job_args("Hello $name", {})]), rc.context)

    assert "exception" in results[0]
    assert "Missing mapping for placeholder: name" in results[0]["exception"]


def test_invalid_template_is_per_job_exception() -> None:
    rc = build_job_context(config={})

    results = handle(build_jobs([_job_args("Hello ${name", {"name": "Alice"})]), rc.context)

    assert "exception" in results[0]
    assert "Invalid template" in results[0]["exception"]
