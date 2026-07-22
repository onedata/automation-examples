"""
A parallel variant of the BagIt data unpacker kept for comparison.
"""

__author__ = "Rafał Widziszewski, Wojciech Szmelich"
__copyright__ = "Copyright (C) 2022-2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import queue
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import Event, Thread
from typing import IO, Final, NamedTuple, TypedDict

from onedata_lambda_utils import (
    DEFAULT_MAX_WORKERS,
    AtmFile,
    AtmObject,
    Job,
    JobContext,
    JobException,
    TimeSeriesMeasurementBuilder,
    mounted_file_path,
    per_job,
)
from onedata_lambda_utils.streaming import ResultStreamer

from bagit_archive import open_mounted_archive


##===================================================================
## Lambda configuration
##===================================================================


READ_CHUNK_SIZE: Final[int] = 10 * 1024**2
PROGRESS_MONITOR_INTERVAL: Final[float] = 1.0
MONITOR_CLEANUP_TIMEOUT: Final[float] = 5.0


##===================================================================
## Lambda interface
##===================================================================


STATS_STREAM: Final[str] = "stats"


class FilesUnpacked(TimeSeriesMeasurementBuilder, ts_name="filesUnpacked", unit=None):
    pass


class BytesUnpacked(TimeSeriesMeasurementBuilder, ts_name="bytesUnpacked", unit="Bytes"):
    pass


class JobArgs(TypedDict):
    archive: AtmFile
    destinationDir: AtmFile


class StatusLog(TypedDict):
    archive: str
    status: str


class JobResult(TypedDict):
    unpackedFiles: list[str]
    statusLog: StatusLog


##===================================================================
## Lambda implementation
##===================================================================


class FileUnpackCtx(NamedTuple):
    job_args: JobArgs
    source_path: str
    data_dir_relative_path: str
    destination_dir: Path
    target_path: Path
    expected_size: int


@dataclass
class FileUnpackProgress:
    target_size: int
    current_size: int = 0


@per_job(max_workers=DEFAULT_MAX_WORKERS)
def handle(job: Job[JobArgs], ctx: JobContext[AtmObject]) -> JobResult:
    archive_file = job.args["archive"]
    destination_dir = job.args["destinationDir"]

    if archive_file["type"] != "REG":
        raise JobException("Archive must be a regular file")
    if destination_dir["type"] != "DIR":
        raise JobException("Destination must be a directory")

    stats = ctx.result_streamer(STATS_STREAM)
    unpacked_files = _unpack_bagit_archive(job.args, stats)

    return {
        "unpackedFiles": unpacked_files,
        "statusLog": {
            "archive": archive_file["name"],
            "status": f"Successfully unpacked {len(unpacked_files)} files.",
        },
    }


def _unpack_bagit_archive(job_args: JobArgs, stats: ResultStreamer) -> list[str]:
    destination_dir = Path(mounted_file_path(job_args["destinationDir"]["fileId"]))
    destination_dir.mkdir(parents=True, exist_ok=True)

    files_to_unpack = _collect_files_to_unpack(job_args, destination_dir)
    files_to_monitor: queue.Queue[tuple[Path, int]] = queue.Queue()
    all_files_unpacked = Event()
    monitor = Thread(
        target=_monitor_files_unpacking,
        daemon=True,
        args=(stats, files_to_monitor, all_files_unpacked),
    )
    monitor.start()

    try:
        with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
            return list(
                executor.map(
                    lambda file_ctx: _unpack_file(file_ctx, files_to_monitor),
                    files_to_unpack,
                )
            )
    finally:
        all_files_unpacked.set()
        monitor.join(timeout=MONITOR_CLEANUP_TIMEOUT)


def _collect_files_to_unpack(
    job_args: JobArgs,
    destination_dir: Path,
) -> list[FileUnpackCtx]:
    files_to_unpack = []

    with open_mounted_archive(job_args["archive"]) as archive:
        data_dir = archive.build_file_path("data", is_dir=True)
        for file_path in archive.list_files():
            if not file_path.startswith(data_dir):
                continue

            file_rel_path = file_path[len(data_dir) :].lstrip("/")
            if not file_rel_path:
                continue

            target_path = _build_safe_target_path(destination_dir, file_rel_path)
            if not archive.is_file(file_path):
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            files_to_unpack.append(
                FileUnpackCtx(
                    job_args=job_args,
                    source_path=file_path,
                    data_dir_relative_path=file_rel_path,
                    destination_dir=destination_dir,
                    target_path=target_path,
                    expected_size=archive.file_size(file_path),
                )
            )

    return files_to_unpack


def _unpack_file(
    file_ctx: FileUnpackCtx,
    files_to_monitor: queue.Queue[tuple[Path, int]],
) -> str:
    files_to_monitor.put((file_ctx.target_path, file_ctx.expected_size))
    file_ctx.target_path.parent.mkdir(parents=True, exist_ok=True)

    with (
        open_mounted_archive(file_ctx.job_args["archive"]) as archive,
        archive.open_file(file_ctx.source_path) as src,
        open(file_ctx.target_path, "wb") as dst,
    ):
        bytes_unpacked = _copy_file(src, dst)

    if bytes_unpacked != file_ctx.expected_size:
        raise JobException(
            f"Failed to unpack {file_ctx.source_path}; expected "
            f"{file_ctx.expected_size} bytes, wrote {bytes_unpacked}"
        )

    return _build_relative_file_dst_path(file_ctx)


def _copy_file(src: IO[bytes], dst: IO[bytes]) -> int:
    bytes_unpacked = 0
    while chunk := src.read(READ_CHUNK_SIZE):
        bytes_written = dst.write(chunk)
        chunk_size = len(chunk)
        if bytes_written != chunk_size:
            raise JobException(
                "Unable to write a data chunk to file; "
                f"written {bytes_written} bytes instead of {chunk_size}."
            )
        bytes_unpacked += chunk_size
    return bytes_unpacked


def _monitor_files_unpacking(
    stats: ResultStreamer,
    files_to_monitor: queue.Queue[tuple[Path, int]],
    all_files_unpacked: Event,
) -> None:
    monitored_files: dict[Path, FileUnpackProgress] = {}
    any_unpacking_ongoing = True

    while any_unpacking_ongoing or not files_to_monitor.empty() or monitored_files:
        any_unpacking_ongoing = not all_files_unpacked.wait(timeout=PROGRESS_MONITOR_INTERVAL)

        while not files_to_monitor.empty():
            file_path, target_size = files_to_monitor.get()
            monitored_files[file_path] = FileUnpackProgress(target_size)

        bytes_unpacked = 0
        files_unpacked = 0
        for file_path, progress in list(monitored_files.items()):
            file_size = _get_file_size(file_path)
            bytes_unpacked += file_size - progress.current_size

            if file_size >= progress.target_size:
                files_unpacked += 1
                monitored_files.pop(file_path)
            else:
                progress.current_size = file_size

        if bytes_unpacked:
            stats.stream_item(BytesUnpacked.build(value=bytes_unpacked))
        if files_unpacked:
            stats.stream_item(FilesUnpacked.build(value=files_unpacked))


def _get_file_size(path: Path) -> int:
    if not path.exists():
        return 0
    return path.stat().st_size


def _build_safe_target_path(destination_dir: Path, file_rel_path: str) -> Path:
    rel_path = PurePosixPath(file_rel_path)
    if _is_unsafe_relative_archive_path(rel_path):
        raise JobException(f"Unsafe archive path: {file_rel_path}")

    target_path = (destination_dir / Path(*rel_path.parts)).resolve()
    resolved_destination = destination_dir.resolve()
    if not target_path.is_relative_to(resolved_destination):
        raise JobException(f"Unsafe archive path: {file_rel_path}")
    return target_path


def _is_unsafe_relative_archive_path(path: PurePosixPath) -> bool:
    return (
        not path.parts or path.is_absolute() or any(part in ("", ".", "..") for part in path.parts)
    )


def _build_relative_file_dst_path(file_ctx: FileUnpackCtx) -> str:
    dst_dir_id = file_ctx.job_args["destinationDir"]["fileId"]
    return f".__onedata__file_id__{dst_dir_id}/{file_ctx.data_dir_relative_path}"
