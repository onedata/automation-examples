"""Shared BagIt archive access helpers."""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import abc
import contextlib
import tarfile
import zipfile
from collections.abc import Generator, Iterable
from pathlib import Path, PurePosixPath
from typing import IO, Final

from onedata_lambda_utils import AtmFile, JobException, mounted_file_path


BAGIT_TXT_PATH_PARTS: Final[int] = 2


class BagitArchive(abc.ABC):
    """Common interface for reading BagIt archives stored as ZIP or TAR files."""

    def __init__(self) -> None:
        self._bagit_dir_name: str | None = None
        self._files: list[str] | None = None
        self._manifest_file_by_algorithm: dict[str, str | None] = {}

    def get_bagit_dir_name(self) -> str:
        """Return the top-level BagIt directory containing the required bagit.txt file."""
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
        """Build an archive-internal path relative to the detected BagIt directory."""
        suffix = self._dir_suffix if is_dir else ""
        return f"{self.get_bagit_dir_name()}/{file_rel_path}{suffix}"

    def list_manifest_files(self, algorithms: Iterable[str]) -> list[str]:
        """Return existing payload manifest files for the requested checksum algorithms."""
        manifests = []
        for algorithm in algorithms:
            manifest_file = self._find_manifest_file(algorithm)
            if manifest_file is not None:
                manifests.append(manifest_file)
        return manifests

    def _find_manifest_file(self, algorithm: str) -> str | None:
        """Return manifest path for one checksum algorithm, caching missing files too."""
        if algorithm in self._manifest_file_by_algorithm:
            return self._manifest_file_by_algorithm[algorithm]

        manifest_file: str | None = self.build_file_path(f"manifest-{algorithm}.txt")
        if manifest_file not in self.list_files():
            manifest_file = None

        self._manifest_file_by_algorithm[algorithm] = manifest_file
        return manifest_file

    def find_fetch_file(self) -> str | None:
        """Return fetch.txt path if the archive declares remote payload files."""
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
        """Return the suffix used when matching directory paths in this archive type."""
        pass

    @abc.abstractmethod
    def list_files(self) -> list[str]:
        """Return all archive member paths."""
        pass

    @abc.abstractmethod
    def open_file(self, path: str) -> IO[bytes]:
        """Open an archive member for binary reading."""
        pass

    @abc.abstractmethod
    def is_file(self, path: str) -> bool:
        """Return whether the archive member is a regular file."""
        pass

    @abc.abstractmethod
    def file_size(self, path: str) -> int:
        """Return the archive member size in bytes."""
        pass


class ZipBagitArchive(BagitArchive):
    """BagIt archive reader backed by zipfile.ZipFile."""

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
    """BagIt archive reader backed by tarfile.TarFile."""

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
    """Open a ZIP/TAR BagIt archive using the file extension from archive_name."""
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
    """Open an archive from the mounted Oneclient filesystem."""
    with open_archive(Path(mounted_file_path(archive["fileId"])), archive["name"]) as bagit_archive:
        yield bagit_archive


def is_unsafe_relative_archive_path(path: PurePosixPath) -> bool:
    return (
        not path.parts or path.is_absolute() or any(part in ("", ".", "..") for part in path.parts)
    )


def extract_safe_data_relative_path(path: str) -> PurePosixPath:
    """Return a safe archive path relative to the BagIt data/ directory."""
    archive_path = PurePosixPath(path)
    if not archive_path.parts or archive_path.parts[0] != "data":
        raise JobException(f"Path must point inside data/ directory: {path}")

    data_relative_path = PurePosixPath(*archive_path.parts[1:])
    if is_unsafe_relative_archive_path(data_relative_path):
        raise JobException(f"Path must point to a safe location inside data/ directory: {path}")

    return data_relative_path
