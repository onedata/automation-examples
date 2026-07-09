"""
Integration tests for the mounted fetch file parser through the SDK runtime.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

from pathlib import Path
from typing import Any

import pytest
from onedata_lambda_utils.testing import build_request, run_local

from parse_fetch_file_mounted.handler import handle


def _job_args() -> dict[str, Any]:
    return {
        "fetchFile": {
            "fileId": "fetch-file-id",
            "name": "fetch.txt",
            "type": "REG",
        },
        "destinationDir": {
            "fileId": "destination-id",
            "type": "DIR",
        },
    }


def _mounted_file(mount_point: Path, file_id: str) -> Path:
    return mount_point / f".__onedata__file_id__{file_id}"


def test_run_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mount_point = tmp_path / "mnt"
    mount_point.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mount_point))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    _mounted_file(mount_point, "fetch-file-id").write_text(
        "https://example.test/a.txt 12 a.txt\nroot://example.test/b.bin 34 nested/b.bin\n"
    )

    result = run_local(handle, build_request([_job_args()], config={}), out_dir=out_dir)

    assert result.envelope == {
        "resultsBatch": [
            {
                "filesToDownload": [
                    {
                        "sourceUrl": "https://example.test/a.txt",
                        "destinationPath": ".__onedata__file_id__destination-id/a.txt",
                        "size": 12,
                    },
                    {
                        "sourceUrl": "root://example.test/b.bin",
                        "destinationPath": ".__onedata__file_id__destination-id/nested/b.bin",
                        "size": 34,
                    },
                ],
                "statusLog": {
                    "severity": "info",
                    "fetchFileName": "fetch.txt",
                    "status": "Found  2 files to be downloaded.",
                },
            }
        ]
    }
