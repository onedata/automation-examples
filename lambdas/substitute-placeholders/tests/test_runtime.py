"""
Integration tests for the placeholder substitution lambda through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

from onedata_lambda_utils.testing import build_request, run_local

from substitute_placeholders.handler import handle


def _job_args(template: str, mappings: dict[str, str]) -> dict[str, Any]:
    return {
        "template": template,
        "mappings": mappings,
    }


def test_run_end_to_end(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    request = build_request(
        [_job_args("Archive ${archive} in $space", {"archive": "bag.zip", "space": "demo"})],
        config={},
    )
    result = run_local(handle, request, out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "output": "Archive bag.zip in demo",
            }
        ]
    }
