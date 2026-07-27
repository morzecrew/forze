"""Conformance battery for the storage plane — the mock and every real object store.

The storage plane has an unusual shape: `forze_s3` and `forze_gcs` are thin subclasses of one
shared `ObjectStorageAdapter`, so real-vs-real divergence lives in the *clients*, while the
in-memory mock is a parallel reimplementation and diverges in the *adapter*. Both kinds bit
here, and probing found three divergences that were real-vs-real — not merely mock-vs-real:

- **delete of a missing key** — S3 answered 204, the GCS API 404. A cleanup path written
  against one raised against the other;
- **a second `abort_upload`** — the port docstring already promised it does not error, and
  one S3 implementation answered ``NoSuchUpload`` anyway. The guarantee was documented and
  unimplemented;
- **copy onto the same key** — AWS S3 and MinIO reject it, other implementations accept it,
  so the outcome of a caller mistake depended on which server was wired.

That last pair came out of running the S3 leg against *two* independent implementations
(MinIO and floci). A single-server suite would have concluded the plane was consistent.

The mock's own divergences were listing order (insertion, where an object store lists
lexicographically — masked while keys are generated, since uuid7 sorts by time) and allowing
a self-copy the real adapter now refuses.

Also pinned here: unconditional `overwrite_stream` on a missing key creates it, while a
*conditional* one raises. That was fixed once — a portability import stumbled into the mock
being stricter than S3/GCS — and never got a test asserting the two agree.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, final

import attrs
import pytest

from forze.application.contracts.storage import SELF_COPY_CODE, UploadedObject
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


async def one_chunk(data: bytes) -> AsyncIterator[bytes]:
    """A single-piece byte stream, for the ``*_stream`` entry points."""

    yield data


@final
@attrs.define(slots=True, kw_only=True)
class StorageHarness:
    """One storage plane under test, plus a key factory unique to the run."""

    cmd: Any
    query: Any

    key: Callable[[str], str]
    """A key unique to this run, so checks are order-independent on a shared bucket."""


Check = Callable[[StorageHarness], Awaitable[None]]


# ....................... #


async def check_deleting_a_missing_object_is_a_no_op(h: StorageHarness) -> None:
    """Idempotent delete, because the backends disagreed and callers should not care."""

    await h.cmd.delete(h.key("never/written.txt"))

    # And deleting a real object twice is equally quiet.
    stored = await h.cmd.upload(UploadedObject(filename="gone.txt", data=b"bye"))
    await h.cmd.delete(stored.key)
    await h.cmd.delete(stored.key)

    with pytest.raises(CoreException) as missing:
        await h.query.head(stored.key)

    assert missing.value.kind is ExceptionKind.NOT_FOUND


# ....................... #


async def check_aborting_an_upload_twice_is_a_no_op(h: StorageHarness) -> None:
    """The port promised this; one S3 implementation did not deliver it."""

    session = await h.cmd.begin_upload(h.key("multi/part.bin"), content_type="text/plain")

    await h.cmd.abort_upload(session)
    await h.cmd.abort_upload(session)


# ....................... #


async def check_copying_onto_the_same_key_is_refused(h: StorageHarness) -> None:
    """Refused identically everywhere, rather than depending on the server."""

    stored = await h.cmd.upload(UploadedObject(filename="self.txt", data=b"abc"))

    with pytest.raises(CoreException) as refused:
        await h.cmd.copy(stored.key, stored.key)

    assert refused.value.code == SELF_COPY_CODE

    # The object is untouched and a real copy still works.
    other = await h.cmd.copy(stored.key, h.key("copies/other.txt"))
    assert other.size == 3


async def check_moving_onto_the_same_key_is_refused(h: StorageHarness) -> None:
    """``move`` inherits the rule, because it is a copy — and a delete.

    Left unguarded it read worse than the copy it wraps: the copy half raised on AWS/MinIO
    and did nothing elsewhere, while the delete half was skipped, so the call "succeeded"
    without moving anything on some servers and failed on others. Same mistake, same code.
    """

    stored = await h.cmd.upload(UploadedObject(filename="selfmove.txt", data=b"abcd"))

    with pytest.raises(CoreException) as refused:
        await h.cmd.move(stored.key, stored.key)

    assert refused.value.code == SELF_COPY_CODE

    # The source survived the refusal, and a real move still works.
    moved = await h.cmd.move(stored.key, h.key("moved/other.txt"))
    assert moved.size == 4


# ....................... #


async def check_listing_is_ordered_by_key(h: StorageHarness) -> None:
    """Lexicographic by key — what an object store returns, and what ``offset`` walks.

    Written with caller-supplied keys on purpose: with generated keys uuid7 sorts by time, so
    insertion order and key order agree and a divergence stays invisible.
    """

    prefix = h.key("ordered")

    for name in ("zeta", "alpha", "mid"):
        await h.cmd.overwrite_stream(
            f"{prefix}/{name}.txt", one_chunk(b"x"), content_type="text/plain"
        )

    listed, total = await h.query.list(limit=10, offset=0, prefix=prefix)

    assert total == 3
    assert [obj.key for obj in listed] == [
        f"{prefix}/alpha.txt",
        f"{prefix}/mid.txt",
        f"{prefix}/zeta.txt",
    ]

    # And ``offset`` walks that same order.
    page, _ = await h.query.list(limit=1, offset=1, prefix=prefix)
    assert [obj.key for obj in page] == [f"{prefix}/mid.txt"]


# ....................... #


async def check_unconditional_overwrite_creates_a_missing_object(h: StorageHarness) -> None:
    """Create-or-replace, as an S3/GCS ``PUT`` does.

    The mock used to raise ``not_found`` here, which a portability import discovered by
    failing to land a blob at its archived key on a fresh backend. The fix never got a test
    asserting the two planes agree; this is it.
    """

    key = f"{h.key('archived')}/blob.bin"

    created = await h.cmd.overwrite_stream(
        key, one_chunk(b"first"), content_type="application/octet-stream"
    )

    assert created.key == key
    assert created.size == 5

    replaced = await h.cmd.overwrite_stream(
        key, one_chunk(b"second-longer"), content_type="application/octet-stream"
    )

    assert replaced.size == len(b"second-longer")

    downloaded = await h.query.download(key)
    assert downloaded.data == b"second-longer"


# ....................... #


async def check_conditional_overwrite_of_a_missing_object_fails(h: StorageHarness) -> None:
    """The other half: asserting an ETag on something that is gone is the delete/overwrite
    race, and must not silently recreate it."""

    with pytest.raises(CoreException) as missing:
        await h.cmd.overwrite_stream(
            f"{h.key('vanished')}/blob.bin",
            one_chunk(b"x"),
            content_type="application/octet-stream",
            if_match='"deadbeef"',
        )

    assert missing.value.kind is ExceptionKind.NOT_FOUND


STORAGE_BATTERY: tuple[Check, ...] = (
    check_deleting_a_missing_object_is_a_no_op,
    check_aborting_an_upload_twice_is_a_no_op,
    check_copying_onto_the_same_key_is_refused,
    check_moving_onto_the_same_key_is_refused,
    check_listing_is_ordered_by_key,
    check_unconditional_overwrite_creates_a_missing_object,
    check_conditional_overwrite_of_a_missing_object_fails,
)
"""Every check. The mock runs them as a unit test; S3 (twice — MinIO and floci) and GCS live."""
