"""
Reads manifests from a BagIt archive and stores expected checksums as xattrs.
"""

__author__ = "Bartosz Walkowicz, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
from pathlib import Path, PurePosixPath
from typing import Final, TypedDict

import xattr
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    mounted_file_path,
    per_job,
)

from bagit_archive import BagitArchive, open_mounted_archive


##===================================================================
## Lambda configuration
##===================================================================


AVAILABLE_CHECKSUM_ALGORITHMS: Final[set[str]] = {"adler32"}.union(hashlib.algorithms_available)


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    archive: AtmFile
    destinationDir: AtmFile


type JobResult = None


##===================================================================
## Lambda implementation
##===================================================================


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], _ctx: JobContext[AtmObject]) -> JobResult:
    archive = job.args["archive"]
    destination_dir = job.args["destinationDir"]

    if archive["type"] != "REG":
        raise JobException("Archive must be a regular file")
    if destination_dir["type"] != "DIR":
        raise JobException("Destination must be a directory")

    with open_mounted_archive(archive) as bagit_archive:
        _process_manifest_files(job.args, bagit_archive)

    return None


def _process_manifest_files(job_args: JobArgs, archive: BagitArchive) -> None:
    files = archive.list_files()
    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        manifest_file = archive.build_file_path(f"manifest-{algorithm}.txt")
        if manifest_file in files:
            _process_manifest_file(job_args, archive, manifest_file, algorithm)


def _process_manifest_file(
    job_args: JobArgs, archive: BagitArchive, manifest_file: str, algorithm: str
) -> None:
    xattr_name = f"checksum.{algorithm}.expected"
    with archive.open_file(manifest_file) as fd:
        for line_num, line in enumerate(fd, start=1):
            checksum, rel_file_path = _parse_manifest_line(manifest_file, line_num, line)
            _set_checksum_xattr(
                _build_safe_destination_file_path(job_args, rel_file_path),
                xattr_name,
                checksum,
            )


def _parse_manifest_line(manifest_file: str, line_num: int, line: bytes) -> tuple[str, str]:
    try:
        decoded_line = line.decode("utf-8").strip("\n")
        checksum, file_path = decoded_line.split(maxsplit=1)
    except Exception as ex:
        raise JobException(
            f"Failed to extract checksum and path from {manifest_file} line number {line_num}"
        ) from ex

    rel_path = _extract_manifest_data_relative_path(file_path, manifest_file, line_num)
    return checksum, str(rel_path)


def _extract_manifest_data_relative_path(
    file_path: str,
    manifest_file: str,
    line_num: int,
) -> PurePosixPath:
    path = PurePosixPath(file_path)
    if not path.parts or path.parts[0] != "data":
        raise JobException(
            f"Manifest path must point inside data/ directory ({manifest_file} line {line_num})"
        )

    rel_path = PurePosixPath(*path.parts[1:])
    if not rel_path.parts:
        raise JobException(
            f"Manifest path must point to a file inside data/ directory "
            f"({manifest_file} line {line_num})"
        )
    return rel_path


def _build_safe_destination_file_path(job_args: JobArgs, rel_file_path: str) -> Path:
    rel_path = PurePosixPath(rel_file_path)
    if _is_unsafe_relative_archive_path(rel_path):
        raise JobException(f"Unsafe manifest path: {rel_file_path}")

    destination_dir = Path(mounted_file_path(job_args["destinationDir"]["fileId"]))
    file_path = (destination_dir / Path(*rel_path.parts)).resolve()
    resolved_destination = destination_dir.resolve()
    if not file_path.is_relative_to(resolved_destination):
        raise JobException(f"Unsafe manifest path: {rel_file_path}")
    return file_path


def _is_unsafe_relative_archive_path(path: PurePosixPath) -> bool:
    return (
        not path.parts or path.is_absolute() or any(part in ("", ".", "..") for part in path.parts)
    )


def _set_checksum_xattr(file_path: Path, xattr_name: str, checksum: str) -> None:
    try:
        xattr.xattr(file_path).set(xattr_name, f'"{checksum}"'.encode())
    except Exception as ex:
        raise JobException(
            f"Failed to set xattr {xattr_name}:{checksum} on file {file_path}: {ex}"
        ) from ex
