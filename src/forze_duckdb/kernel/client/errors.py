"""Object-store and file failures of a DuckDB read, in the core exception taxonomy.

A query that scans ``s3://`` / ``gs://`` / ``http(s)://`` or local files fails with
``duckdb.IOException``, or its subclass ``duckdb.HTTPException`` when the store answered with a
status, and with ``duckdb.InvalidInputException`` when a Parquet file is not one.
:func:`object_store_error` translates those; every other driver error is left to its caller.
The analytics adapter applies it on every read path. The dynamic-read adapter keeps its own
translation (RFC 0078 §4.5), and the routed client's attestation reads driver classes.
"""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import re
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit

import duckdb

from forze.base.exceptions import CoreException, exc

# ----------------------- #

SOURCE_NOT_FOUND_CODE = "duckdb_source_not_found"
"""Code of the ``not_found`` error for a file, object or glob that matched nothing."""

SOURCE_BUCKET_NOT_FOUND_CODE = "duckdb_source_bucket_not_found"
"""Code of the ``configuration`` error for a bucket the store does not have."""

SOURCE_ACCESS_DENIED_CODE = "duckdb_source_access_denied"
"""Code of the ``infrastructure`` error for a 401 or 403 from the store."""

SOURCE_THROTTLED_CODE = "duckdb_source_throttled"
"""Code of the ``throttled`` error for a 429 from the store."""

SOURCE_UNAVAILABLE_CODE = "duckdb_source_unavailable"
"""Code of the ``infrastructure`` error for every other failed read: 5xx, timeout, refused
connection, local IO."""

SOURCE_CORRUPT_CODE = "duckdb_source_corrupt"
"""Code of the ``internal`` error for a file DuckDB cannot read as Parquet."""

_NO_FILES = "No files found that match the pattern"
"""DuckDB's ``IOException`` for a path or glob that matched no file, on every filesystem."""

_NO_SUCH_BUCKET = "<Code>NoSuchBucket</Code>"
"""S3's error body for a missing bucket. A ``HEAD`` has no body, so a missing bucket under a
single-object path still reads as a missing object."""

_NOT_PARQUET = (
    "too small to be a Parquet file",
    "No magic bytes found at end of file",
    "Footer length error in file",
)
"""``InvalidInputException`` messages for an empty, truncated or non-Parquet file, the same on
DuckDB 1.5 and 2.0. Any other invalid input, such as a failed cast, is the statement's."""

_QUOTED = re.compile(r"""['"]([^'"]+)['"]""")
"""The first quoted token of the message's first line: the path or URL the read failed on."""

# ....................... #


def _location(message: str) -> str | None:
    """The failed path from *message*, without a query string, fragment or user info.

    A presigned URL carries its signature in the query, and an ``http`` URL can carry
    credentials before the host; neither belongs in an error. A path that does not parse as
    a URL is left out rather than reported raw.
    """

    match = _QUOTED.search(message.partition("\n")[0])

    if match is None:
        return None

    try:
        parts = urlsplit(match.group(1))

    except ValueError:  # e.g. an unclosed ``[`` read as an IPv6 host
        return None

    return urlunsplit((parts.scheme, parts.netloc.rpartition("@")[2], parts.path, "", ""))


# ....................... #


def object_store_error(error: duckdb.Error) -> CoreException | None:
    """Translate a failed object-store or file read into a :class:`CoreException`.

    A ``404`` or a pattern that matched no file is the caller's miss, ``not_found``; a missing
    bucket is ``configuration``; ``429`` is ``throttled``. A file that is not Parquet is
    ``internal``: a re-read returns the same bytes, and a healthy store must not count against
    a circuit breaker. Every other IO failure, ``401`` and ``403`` included (vended or rotated
    credentials recover), is ``infrastructure``, so a retry policy may take another attempt.
    The details name the path and the status, never the response's headers or body, and are
    not shown to clients.

    :param error: The driver's exception; the caller chains it as ``__cause__``.
    :returns: The error to raise, or ``None`` when *error* is not a failed read.
    """

    message = str(error)

    if isinstance(error, duckdb.InvalidInputException):
        if not any(marker in message for marker in _NOT_PARQUET):
            return None

        mapped = exc.internal(
            "DuckDB source is not a readable Parquet file.",
            code=SOURCE_CORRUPT_CODE,
            details={"path": _location(message)},
        )
        mapped.expose_details = False

        return mapped

    if not isinstance(error, duckdb.IOException):
        return None

    status = getattr(error, "status_code", None)  # set on an ``HTTPException`` by the driver
    details: dict[str, object] = {"path": _location(message)}

    if status is not None:
        details["status"] = status

    if status == 404 and _NO_SUCH_BUCKET in str(getattr(error, "body", "")):
        mapped = exc.configuration(
            "DuckDB source bucket not found.",
            code=SOURCE_BUCKET_NOT_FOUND_CODE,
            details=details,
        )

    elif status == 404 or (status is None and _NO_FILES in message):
        mapped = exc.not_found(
            "DuckDB source not found.",
            code=SOURCE_NOT_FOUND_CODE,
            details=details,
        )

    elif status in {401, 403}:
        mapped = exc.infrastructure(
            "DuckDB source access denied.",
            code=SOURCE_ACCESS_DENIED_CODE,
            details=details,
        )

    elif status == 429:
        mapped = exc.throttled(
            "DuckDB source request throttled.",
            code=SOURCE_THROTTLED_CODE,
            details=details,
        )

    else:
        mapped = exc.infrastructure(
            "DuckDB source read failed.",
            code=SOURCE_UNAVAILABLE_CODE,
            details=details,
        )

    # The path is the backend's context, for the server's logs.
    mapped.expose_details = False

    return mapped


@contextmanager
def object_store_errors() -> Iterator[None]:
    """Raise a failed read from the block as :func:`object_store_error`'s error."""

    try:
        yield

    except duckdb.Error as error:
        mapped = object_store_error(error)

        if mapped is None:
            raise

        raise mapped from error


async def object_store_stream[T](chunks: AsyncIterator[T]) -> AsyncGenerator[T]:
    """Yield *chunks*, translating a failed read only while the next one is fetched.

    The ``yield`` stays outside :func:`object_store_errors`, so what the consumer throws in
    comes back as it was.
    """

    while True:
        with object_store_errors():
            try:
                chunk = await anext(chunks)

            except StopAsyncIteration:
                return

        yield chunk
