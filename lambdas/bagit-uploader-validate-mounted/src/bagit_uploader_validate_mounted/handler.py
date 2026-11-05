"""
A lambda which validates BagIt archives.
"""

__author__ = "Rafał Widziszewski, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import re
import traceback
from pathlib import Path
from typing import Final, TypedDict

from onedata_lambda_sdk import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    per_job,
)

from bagit_archive import BagitArchive, open_mounted_archive
from checksum import (
    AVAILABLE_CHECKSUM_ALGORITHMS,
    ChecksumAlgorithm,
    calculate_checksum,
    require_supported,
)


##===================================================================
## Lambda configuration
##===================================================================


SUPPORTED_URL_SCHEMAS: Final[tuple[str, ...]] = ("root:", "http:", "https:")
READ_CHUNK_SIZE: Final[int] = 10 * 1024**2
BAGIT_TXT_LINES: Final[int] = 2


##===================================================================
## Lambda interface
##===================================================================


class JobArgs(TypedDict):
    archive: AtmFile


class JobResult(TypedDict):
    validArchives: list[AtmFile]
    statusLog: AtmObject


##===================================================================
## Lambda implementation
##===================================================================


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], _ctx: JobContext[AtmObject]) -> JobResult:
    archive = job.args["archive"]
    if archive["type"] != "REG":
        raise JobException("Not an archive file")

    try:
        with open_mounted_archive(archive) as bagit_archive:
            _assert_valid_archive(bagit_archive)
    except JobException as ex:
        return {
            "validArchives": [],
            "statusLog": {
                "archive": archive["name"],
                "status": "Invalid bagit archive",
                "reason": str(ex),
            },
        }
    except Exception:
        return {
            "validArchives": [],
            "statusLog": {
                "archive": archive["name"],
                "status": "Failed to validate bagit archive",
                "reason": traceback.format_exc(),
            },
        }

    return {
        "validArchives": [archive],
        "statusLog": {
            "archive": archive["name"],
            "status": "Valid bagit archive",
        },
    }


def _assert_valid_archive(archive: BagitArchive) -> None:
    _validate_structure(archive)

    # Optional elements are checked first as they may contain checksums of required files.
    _validate_any_tagmanifest_file(archive)

    _validate_bagit_txt(archive)
    _validate_payload(archive)


def _validate_structure(archive: BagitArchive) -> None:
    files = archive.list_files()
    if archive.build_file_path("bagit.txt") not in files:
        raise JobException("bagit.txt file not found")

    if not archive.list_manifest_files(AVAILABLE_CHECKSUM_ALGORITHMS):
        raise JobException("No manifest file found")

    if archive.build_file_path("data", is_dir=True) not in files:
        raise JobException("Payload (data/) directory not found")


def _validate_any_tagmanifest_file(archive: BagitArchive) -> None:
    files = archive.list_files()
    for algorithm in AVAILABLE_CHECKSUM_ALGORITHMS:
        tagmanifest_file = archive.build_file_path(f"tagmanifest-{algorithm}.txt")
        if tagmanifest_file not in files:
            continue

        for exp_checksum, file_rel_path in _parse_manifest_file(tagmanifest_file, archive):
            file_path = archive.build_file_path(file_rel_path)
            if file_path not in files:
                raise JobException(f"{file_path} referenced by {tagmanifest_file} not found")

            _validate_file_checksum(archive, file_path, algorithm, exp_checksum)

        return


def _validate_file_checksum(
    archive: BagitArchive, file_path: str, algorithm: ChecksumAlgorithm, exp_checksum: str
) -> None:
    with archive.open_file(file_path) as fd:
        data_stream = iter(lambda: fd.read(READ_CHUNK_SIZE), b"")
        checksum = calculate_checksum(algorithm, data_stream)

    if checksum != exp_checksum:
        raise JobException(
            f"{algorithm} checksum verification failed for {file_path}.\n"
            f"Expected: {exp_checksum}, Calculated: {checksum}"
        )


def _validate_bagit_txt(archive: BagitArchive) -> None:
    with archive.open_file(archive.build_file_path("bagit.txt")) as fd:
        lines = fd.readlines()

    if len(lines) != BAGIT_TXT_LINES:
        raise JobException("Invalid bagit.txt format")

    if not re.match(r"^\s*BagIt-Version: [0-9]+.[0-9]+\s*$", lines[0].decode("utf-8")):
        raise JobException(
            "Invalid 'Tag-File-Character-Encoding' definition in 1st line in bagit.txt"
        )
    if not re.match(r"^\s*Tag-File-Character-Encoding: \w+", lines[1].decode("utf-8")):
        raise JobException(
            "Invalid 'Tag-File-Character-Encoding' definition in 2nd line in bagit.txt"
        )


def _validate_payload(archive: BagitArchive) -> None:
    bagit_dir = archive.get_bagit_dir_name()
    data_dir = f"{bagit_dir}/data/"

    payload_files = set()
    for file_path in archive.list_files():
        if (
            file_path.startswith(data_dir)
            and len(file_path) > len(data_dir)
            and archive.is_file(file_path)
        ):
            payload_files.add(file_path[len(bagit_dir) + 1 :])

    payload_files.update(_parse_fetch_file(archive))

    for manifest_file in archive.list_manifest_files(AVAILABLE_CHECKSUM_ALGORITHMS):
        referenced_files = set()
        for _exp_checksum, path in _parse_manifest_file(manifest_file, archive):
            referenced_files.add(path)
            # Currently skipping verifying payload checksums declared in manifests.
            # algorithm = _manifest_algorithm(manifest_file)
            # _validate_file_checksum(
            #     archive,
            #     archive.build_file_path(path),
            #     algorithm,
            #     exp_checksum,
            # )

        if payload_files != referenced_files:
            raise JobException(
                f"Files referenced by {manifest_file} do not match with payload files.\n"
                f"  Files in payload but not referenced: {payload_files - referenced_files}\n"
                f"  Files referenced but not in payload: {referenced_files - payload_files}"
            )


def _manifest_algorithm(manifest_file: str) -> ChecksumAlgorithm:
    manifest_name = Path(manifest_file).name
    return require_supported(manifest_name.removeprefix("manifest-").removesuffix(".txt"))


def _parse_manifest_file(manifest_file: str, archive: BagitArchive) -> list[tuple[str, str]]:
    checksums = []
    with archive.open_file(manifest_file) as fd:
        for line_num, line in enumerate(fd, start=1):
            try:
                decoded_line = line.decode("utf-8").strip("\n")
                checksum, file_path = decoded_line.split(maxsplit=1)
            except Exception as ex:
                raise JobException(
                    f"Failed to parse line number {line_num} in {manifest_file} file"
                ) from ex

            checksums.append((checksum, file_path))

    return checksums


def _parse_fetch_file(archive: BagitArchive) -> list[str]:
    fetch_file = archive.build_file_path("fetch.txt")

    if fetch_file not in archive.list_files():
        return []

    files_to_download = []
    with archive.open_file(fetch_file) as fd:
        for line_num, line in enumerate(fd, start=1):
            try:
                decoded_line = line.decode("utf-8").strip("\n")
                url, size, path = decoded_line.split(maxsplit=2)
                assert size.isnumeric()
            except Exception as ex:
                raise JobException(
                    f"Failed to extract url, size and path from line number {line_num} in fetch.txt"
                ) from ex

            if not path.startswith("data/"):
                raise JobException(
                    f"File path not within data/ directory (fetch.txt line {line_num})"
                )
            if not any(url.startswith(schema) for schema in SUPPORTED_URL_SCHEMAS):
                raise JobException(f"URL from line number {line_num} in fetch.txt is not supported")

            files_to_download.append(path)

    return files_to_download
