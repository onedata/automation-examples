"""
A parallel variant of the mounted checksum verifier kept for comparison.
"""

__author__ = "Rafał Widziszewski, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2023-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import queue
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Thread
from typing import Final, NamedTuple, TypedDict, cast

import xattr
from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmObject,
    Job,
    JobContext,
    JobException,
    mount_point,
    per_job,
)
from onedata_lambda_utils.streaming import ResultStreamer

from checksum import ChecksumAlgorithm, assert_supported, calculate_checksum


READ_CHUNK_SIZE: Final[int] = 10 * 1024**2
PROGRESS_MONITOR_INTERVAL: Final[float] = 1.0
MONITOR_CLEANUP_TIMEOUT: Final[float] = 5.0
EXPECTED_CHECKSUM_XATTR: Final[re.Pattern[str]] = re.compile(
    r"^checksum\.(?P<algorithm>[^.]+)\.expected$"
)


class JobArgs(TypedDict):
    filePath: str


class ChecksumStatus(TypedDict):
    expected: str
    calculated: str
    status: str


class JobChecksumsReport(TypedDict):
    filePath: str
    checksums: dict[str, ChecksumStatus]


class JobResult(TypedDict):
    result: JobChecksumsReport


class ExpectedFileChecksum(NamedTuple):
    file_path: Path
    algorithm: str
    checksum: str


class CalculatedFileChecksum(NamedTuple):
    algorithm: str
    status: ChecksumStatus


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    file_path = _build_safe_mount_relative_path(job.args["filePath"])
    expected_checksums = _list_expected_checksums(file_path)

    measurements: queue.Queue[dict[str, int | str]] = queue.Queue()
    all_checksums_processed = Event()
    monitor = Thread(
        target=_monitor_checksums,
        daemon=True,
        args=(ctx.result_streamer("stats"), measurements, all_checksums_processed),
    )
    monitor.start()

    try:
        with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
            calculated_checksums = list(
                executor.map(
                    lambda expected: _verify_file_checksum(expected, measurements),
                    expected_checksums,
                )
            )
    finally:
        all_checksums_processed.set()
        monitor.join(timeout=MONITOR_CLEANUP_TIMEOUT)

    return {
        "result": {
            "filePath": str(file_path),
            "checksums": {checksum.algorithm: checksum.status for checksum in calculated_checksums},
        }
    }


def _build_safe_mount_relative_path(file_path: str) -> Path:
    relative_path = Path(file_path)
    if relative_path.is_absolute():
        raise JobException("File path must be relative")

    mount = Path(mount_point()).resolve()
    resolved_path = (mount / relative_path).resolve()
    if not resolved_path.is_relative_to(mount):
        raise JobException("File path must stay within the Oneclient mount point")
    return resolved_path


def _list_expected_checksums(file_path: Path) -> list[ExpectedFileChecksum]:
    expected_checksums = []
    file_xattrs = xattr.xattr(file_path)

    for xattr_name in file_xattrs.list():
        decoded_xattr_name = _decode_xattr_name(xattr_name)
        if not (match := EXPECTED_CHECKSUM_XATTR.match(decoded_xattr_name)):
            continue

        expected_checksums.append(
            ExpectedFileChecksum(
                file_path=file_path,
                algorithm=match.group("algorithm"),
                checksum=_decode_xattr_value(file_xattrs.get(xattr_name)),
            )
        )

    return expected_checksums


def _verify_file_checksum(
    expected: ExpectedFileChecksum,
    measurements: queue.Queue[dict[str, int | str]],
) -> CalculatedFileChecksum:
    calculated_checksum = _calculate_checksum(
        expected.file_path,
        expected.algorithm,
        measurements,
    )
    _set_file_xattr(
        expected.file_path,
        f"checksum.{expected.algorithm}.calculated",
        calculated_checksum,
    )
    _assert_expected_checksum(calculated_checksum, expected.checksum)

    return CalculatedFileChecksum(
        expected.algorithm,
        {
            "expected": expected.checksum,
            "calculated": calculated_checksum,
            "status": "ok",
        },
    )


def _calculate_checksum(
    file_path: Path,
    algorithm: str,
    measurements: queue.Queue[dict[str, int | str]],
) -> str:
    try:
        return _calculate_checksum_insecure(file_path, algorithm, measurements)
    except Exception as ex:
        raise JobException(f"Failed to calculate checksum due to: {ex}") from ex


def _calculate_checksum_insecure(
    file_path: Path,
    algorithm: str,
    measurements: queue.Queue[dict[str, int | str]],
) -> str:
    assert_supported(algorithm)
    with open(file_path, "rb") as file:
        return calculate_checksum(
            cast(ChecksumAlgorithm, algorithm),
            iter(lambda: file.read(READ_CHUNK_SIZE), b""),
            on_bytes=lambda value: measurements.put(
                _build_time_series_measurement(algorithm, value)
            ),
        )


def _build_time_series_measurement(algorithm: str, value: int) -> dict[str, int | str]:
    return {
        "tsName": f"bytesProcessed_{algorithm}",
        "timestamp": int(time.time()),
        "value": value,
    }


def _monitor_checksums(
    stats: ResultStreamer,
    measurements: queue.Queue[dict[str, int | str]],
    all_checksums_processed: Event,
) -> None:
    any_checksum_ongoing = True
    while any_checksum_ongoing or not measurements.empty():
        any_checksum_ongoing = not all_checksums_processed.wait(timeout=PROGRESS_MONITOR_INTERVAL)

        while not measurements.empty():
            stats.stream_item(measurements.get())


def _decode_xattr_name(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _decode_xattr_value(value: str | bytes) -> str:
    decoded = value.decode("utf-8") if isinstance(value, bytes) else value
    return decoded.strip('"')


def _set_file_xattr(file_path: Path, xattr_name: str, xattr_value: str) -> None:
    try:
        xattr.xattr(file_path).set(xattr_name, f'"{xattr_value}"'.encode())
    except Exception as ex:
        raise JobException(f"Failed to set xattr {xattr_name}:{xattr_value}: {ex}") from ex


def _assert_expected_checksum(checksum: str, expected_checksum: str) -> None:
    if checksum != expected_checksum:
        raise JobException(
            f"Expected file checksum: {expected_checksum}, when calculated checksum is: {checksum}"
        )
