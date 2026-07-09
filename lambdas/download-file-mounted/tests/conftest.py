"""
Shared fixtures and fakes for the mounted download lambda tests.
"""

__author__ = "Wojciech, Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import sys
import types
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import requests


def _install_xrootd_stub() -> None:
    xrootd = types.ModuleType("XRootD")
    client = types.ModuleType("XRootD.client")
    flags = types.ModuleType("XRootD.client.flags")

    class _File:
        pass

    class _OpenFlags:
        READ = object()

    client.File = _File
    flags.OpenFlags = _OpenFlags
    xrootd.client = client

    sys.modules.setdefault("XRootD", xrootd)
    sys.modules.setdefault("XRootD.client", client)
    sys.modules.setdefault("XRootD.client.flags", flags)


_install_xrootd_stub()


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


@pytest.fixture
def response_cls() -> type[Response]:
    return Response


@pytest.fixture
def job_args() -> Callable[[str, str, int], dict[str, Any]]:
    return _job_args


def _job_args(source_url: str, destination_path: str, size: int) -> dict[str, Any]:
    return {
        "downloadInfo": {
            "sourceUrl": source_url,
            "destinationPath": destination_path,
            "size": size,
        }
    }
