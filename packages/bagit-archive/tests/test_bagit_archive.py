"""
Unit tests for shared BagIt archive helpers.
"""

__author__ = "Wojciech Szmelich"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import io
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

import pytest
from onedata_lambda_utils import JobException

from bagit_archive import extract_safe_data_relative_path, open_archive


def test_zip_archive_helpers(tmp_path: Path) -> None:
    archive_path = tmp_path / "bag.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("bag/bagit.txt", b"BagIt-Version: 0.97\n")
        archive.writestr("bag/data/", b"")
        archive.writestr("bag/data/file.txt", b"content")
        archive.writestr("bag/manifest-sha256.txt", b"checksum data/file.txt\n")
        archive.writestr("bag/fetch.txt", b"https://example.test/file 7 data/file.txt\n")

    with open_archive(archive_path, "bag.zip") as archive:
        assert archive.get_bagit_dir_name() == "bag"
        assert archive.build_file_path("data", is_dir=True) == "bag/data/"
        assert archive.find_fetch_file() == "bag/fetch.txt"
        assert archive.list_manifest_files(["sha256", "md5"]) == ["bag/manifest-sha256.txt"]
        assert archive.list_manifest_files(["md5", "sha256"]) == ["bag/manifest-sha256.txt"]
        assert archive.is_file("bag/data/file.txt")
        assert archive.file_size("bag/data/file.txt") == 7


def test_tar_archive_helpers(tmp_path: Path) -> None:
    archive_path = tmp_path / "bag.tar"
    with tarfile.open(archive_path, "w") as archive:
        data_dir = tarfile.TarInfo("bag/data")
        data_dir.type = tarfile.DIRTYPE
        archive.addfile(data_dir)
        for name, content in {
            "bag/bagit.txt": b"BagIt-Version: 0.97\n",
            "bag/data/file.txt": b"content",
            "bag/manifest-md5.txt": b"checksum data/file.txt\n",
        }.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))

    with open_archive(archive_path, "bag.tar") as archive:
        assert archive.get_bagit_dir_name() == "bag"
        assert archive.build_file_path("data", is_dir=True) == "bag/data"
        assert archive.find_fetch_file() is None
        assert archive.list_manifest_files(["sha256", "md5"]) == ["bag/manifest-md5.txt"]
        assert archive.is_file("bag/data/file.txt")
        assert archive.file_size("bag/data/file.txt") == 7


def test_extract_safe_data_relative_path() -> None:
    assert extract_safe_data_relative_path("data/dir/file.txt") == PurePosixPath("dir/file.txt")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/data/file.txt",
        "file.txt",
        "data",
        "data/../file.txt",
    ],
)
def test_extract_safe_data_relative_path_rejects_invalid_path(path: str) -> None:
    with pytest.raises(JobException):
        extract_safe_data_relative_path(path)
