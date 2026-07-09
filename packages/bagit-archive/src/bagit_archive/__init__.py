"""Shared BagIt archive access helpers."""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import abc
import contextlib
import tarfile
import zipfile
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import IO, Final

from onedata_lambda_utils import AtmFile, JobException, mounted_file_path


BAGIT_TXT_PATH_PARTS: Final[int] = 2


class BagitArchive(abc.ABC):
    def __init__(self) -> None:
        self._bagit_dir_name: str | None = None
        self._files: list[str] | None = None
        self._manifest_files: dict[frozenset[str], list[str]] = {}

    def get_bagit_dir_name(self) -> str:
        if self._bagit_dir_name is not None:
            return self._bagit_dir_name

        for file_path in self.list_files():
            path_tokens = file_path.split("/")
            if len(path_tokens) == BAGIT_TXT_PATH_PARTS and path_tokens[1] == "bagit.txt":
                bagit_dir_name = path_tokens[0]
                self._bagit_dir_name = bagit_dir_name
                return bagit_dir_name

        raise JobException("Bagit directory not found")

    def build_file_path(self, file_rel_path: str, *, is_dir: bool = False) -> str:
        suffix = self._dir_suffix if is_dir else ""
        return f"{self.get_bagit_dir_name()}/{file_rel_path}{suffix}"

    def list_manifest_files(self, algorithms: Iterable[str]) -> list[str]:
        algorithm_set = frozenset(algorithms)
        if algorithm_set in self._manifest_files:
            return self._manifest_files[algorithm_set]

        manifests = []
        files = self.list_files()
        for algorithm in algorithm_set:
            manifest_file = self.build_file_path(f"manifest-{algorithm}.txt")
            if manifest_file in files:
                manifests.append(manifest_file)

        self._manifest_files[algorithm_set] = manifests
        return manifests

    def find_fetch_file(self) -> str | None:
        all_files = self.list_files()
        for file_path in all_files:
            path_tokens = file_path.split("/")
            if len(path_tokens) == BAGIT_TXT_PATH_PARTS and path_tokens[1] == "bagit.txt":
                fetch_file = f"{path_tokens[0]}/fetch.txt"
                return fetch_file if fetch_file in all_files else None
        return None

    @property
    @abc.abstractmethod
    def _dir_suffix(self) -> str:
        pass

    @abc.abstractmethod
    def list_files(self) -> list[str]:
        pass

    @abc.abstractmethod
    def open_file(self, path: str) -> IO[bytes]:
        pass

    @abc.abstractmethod
    def is_file(self, path: str) -> bool:
        pass

    @abc.abstractmethod
    def file_size(self, path: str) -> int:
        pass


class ZipBagitArchive(BagitArchive):
    def __init__(self, archive: zipfile.ZipFile) -> None:
        super().__init__()
        self.archive = archive

    @property
    def _dir_suffix(self) -> str:
        return "/"

    def list_files(self) -> list[str]:
        if self._files is not None:
            return self._files

        files = self.archive.namelist()
        self._files = files
        return files

    def open_file(self, path: str) -> IO[bytes]:
        return self.archive.open(path)

    def is_file(self, path: str) -> bool:
        return not self.archive.getinfo(path).is_dir()

    def file_size(self, path: str) -> int:
        return self.archive.getinfo(path).file_size


class TarBagitArchive(BagitArchive):
    def __init__(self, archive: tarfile.TarFile) -> None:
        super().__init__()
        self.archive = archive

    @property
    def _dir_suffix(self) -> str:
        return ""

    def list_files(self) -> list[str]:
        if self._files is not None:
            return self._files

        files = self.archive.getnames()
        self._files = files
        return files

    def open_file(self, path: str) -> IO[bytes]:
        fd = self.archive.extractfile(path)
        if fd is None:
            raise JobException(f"Couldn't open {path} in archive")
        return fd

    def is_file(self, path: str) -> bool:
        return self.archive.getmember(path).isfile()

    def file_size(self, path: str) -> int:
        return self.archive.getmember(path).size


@contextlib.contextmanager
def open_archive(archive_path: Path, archive_name: str) -> Generator[BagitArchive]:
    archive_type = Path(archive_name).suffix

    if archive_type == ".zip":
        with zipfile.ZipFile(archive_path) as archive:
            yield ZipBagitArchive(archive)
    elif archive_type == ".tar":
        with tarfile.open(archive_path) as archive:
            yield TarBagitArchive(archive)
    elif archive_type in (".tgz", ".gz"):
        with tarfile.open(archive_path, "r:gz") as archive:
            yield TarBagitArchive(archive)
    else:
        raise JobException(f"Unsupported archive type: {archive_type}")


@contextlib.contextmanager
def open_mounted_archive(archive: AtmFile) -> Generator[BagitArchive]:
    with open_archive(Path(mounted_file_path(archive["fileId"])), archive["name"]) as bagit_archive:
        yield bagit_archive
