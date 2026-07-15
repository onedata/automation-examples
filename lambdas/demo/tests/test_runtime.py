"""
Integration tests for the demo lambda through the SDK runtime.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path

from onedata_lambda_utils.testing import build_request, run_local

from demo.handler import handle


FILE_ID = "file-id"
FILE_NAME = "example.txt"


def test_run_end_to_end(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    request = build_request(
        [
            {
                "item": {
                    "fileId": FILE_ID,
                    "name": FILE_NAME,
                    "type": "REG",
                }
            }
        ],
        config={},
    )

    result = run_local(handle, request, out_dir=out_dir)

    assert result.envelope == {"resultsBatch": [{"result": f"Hello - {FILE_NAME}"}]}
