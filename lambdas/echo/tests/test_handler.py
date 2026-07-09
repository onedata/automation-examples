"""
Unit tests for the echo lambda.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import pytest
from onedata_lambda_utils.testing import build_job_context, build_jobs

from echo import handler


def _config(
    *,
    stream_results: bool = False,
    exception_probability: float = 0,
    wrap_result_in_array: bool | None = None,
) -> dict[str, object]:
    config: dict[str, object] = {
        "sleepDurationSec": 0,
        "exceptionProbability": exception_probability,
        "streamResults": stream_results,
    }
    if wrap_result_in_array is not None:
        config["wrapResultInArray"] = wrap_result_in_array
    return config


def test_returns_input_args(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler.random, "random", lambda: 1)
    rc = build_job_context(config=_config())

    results = handler.handle(build_jobs([{"value": "text", "count": 2}]), rc.context)

    assert results == [{"value": "text", "count": 2}]


def test_wraps_result_values_in_arrays(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler.random, "random", lambda: 1)
    rc = build_job_context(config=_config(wrap_result_in_array=True))

    results = handler.handle(build_jobs([{"value": "text", "count": 2}]), rc.context)

    assert results == [{"value": ["text"], "count": [2]}]


def test_streams_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler.random, "random", lambda: 1)
    rc = build_job_context(config=_config(stream_results=True))

    results = handler.handle(build_jobs([{"value": "text", "count": 2}]), rc.context)

    assert results == [None]
    assert rc.streams == {
        "value": ["text"],
        "count": [2],
    }


def test_returns_random_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(handler.random, "random", lambda: 0)
    rc = build_job_context(config=_config(exception_probability=1))

    results = handler.handle(build_jobs([{"value": "text"}]), rc.context)

    assert results == [{"exception": "Random exception"}]
