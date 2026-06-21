"""Shared checksum logic for Onedata Automation lambdas."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2026 Onedata (onedata.org)"
__license__ = "This software is released under the MIT license cited in LICENSE.txt"

import hashlib
import zlib
from collections.abc import Callable, Iterable
from typing import Final, Literal, get_args

from onedata_lambda_utils import JobException


# Plain (non-PEP 695) alias on purpose: `get_args` reads members off a `Literal` directly.
ChecksumAlgorithm = Literal[
    "adler32",
    "blake2b",
    "blake2s",
    "md5",
    "sha1",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "sha3_224",
    "sha3_256",
    "sha3_384",
    "sha3_512",
    "shake_128",
    "shake_256",
]

AVAILABLE_CHECKSUM_ALGORITHMS: Final[frozenset[str]] = frozenset(get_args(ChecksumAlgorithm))

#: shake_* are extendable-output functions: their `hexdigest()` needs an explicit length.
_SHAKE_DIGEST_BYTES: Final[int] = 32


def is_supported(algorithm: str) -> bool:
    return algorithm in AVAILABLE_CHECKSUM_ALGORITHMS


def assert_supported(algorithm: str) -> None:
    """
    Fail the job (cleanly, no traceback) if `algorithm` is not in the catalogue.

    Raises `JobException`, which the SDK turns into an `AtmException` carrying just the
    message -- the natural fit for a known configuration error.
    """
    if not is_supported(algorithm):
        raise JobException(
            f"{algorithm} algorithm is unsupported. "
            f"Available ones are: {sorted(AVAILABLE_CHECKSUM_ALGORITHMS)}"
        )


def calculate_checksum(
    algorithm: ChecksumAlgorithm,
    chunks: Iterable[bytes],
    *,
    on_bytes: Callable[[int], None] | None = None,
) -> str:
    """
    Compute `algorithm` over a stream of byte `chunks`.

    `on_bytes(len(chunk))` is called after each chunk -- a lambda passes a callback that
    streams a `bytesProcessed` measurement, so progress reporting stays out of the core.
    """
    if algorithm == "adler32":
        value = 1
        for chunk in chunks:
            value = zlib.adler32(chunk, value)
            if on_bytes is not None:
                on_bytes(len(chunk))
        return format(value, "x")

    digest = getattr(hashlib, algorithm)()
    for chunk in chunks:
        digest.update(chunk)
        if on_bytes is not None:
            on_bytes(len(chunk))

    # digest is Any (getattr-based dispatch, so the shake hexdigest(len) branch type-checks);
    # str() pins the declared return type.
    if algorithm in ("shake_128", "shake_256"):
        return str(digest.hexdigest(_SHAKE_DIGEST_BYTES))
    return str(digest.hexdigest())
