"""
Integration tests for the echo lambda through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from echo import handler


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler.random, "random", lambda: 1)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    request = build_request(
        [{"value": "text", "count": 2}],
        config={
            "sleepDurationSec": 0,
            "exceptionProbability": 0,
            "streamResults": False,
        },
    )

    result = run_local(handler.handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [{"value": "text", "count": 2}]}
