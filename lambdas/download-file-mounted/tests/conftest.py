"""
Shared fixtures and fakes for the mounted download lambda tests.
"""

__author__ = "Wojciech, Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import requests


@pytest.fixture
def xrootd_client(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    xrootd = types.ModuleType("XRootD")
    client = types.ModuleType("XRootD.client")
    flags = types.ModuleType("XRootD.client.flags")

    class _OpenFlags:
        READ = object()

    flags.__dict__["OpenFlags"] = _OpenFlags
    xrootd.__dict__["client"] = client

    monkeypatch.setitem(sys.modules, "XRootD", xrootd)
    monkeypatch.setitem(sys.modules, "XRootD.client", client)
    monkeypatch.setitem(sys.modules, "XRootD.client.flags", flags)
    return client


class Response:
    def __init__(self, chunks: list[bytes], status_code: int = 200) -> None:
        self._chunks = chunks
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def iter_content(self, chunk_size: int) -> Iterator[bytes]:
        yield from self._chunks


@pytest.fixture
def mount_point(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    mp = tmp_path / "mnt"
    mp.mkdir()
    monkeypatch.setenv("ONECLIENT_MOUNT_POINT", str(mp))
    return mp


@pytest.fixture
def out_dir(tmp_path: Path) -> Path:
    out = tmp_path / "out"
    out.mkdir()
    return out


def job_args(source_url: str, destination_path: str, size: int) -> dict[str, Any]:
    return {
        "downloadInfo": {
            "sourceUrl": source_url,
            "destinationPath": destination_path,
            "size": size,
        }
    }
