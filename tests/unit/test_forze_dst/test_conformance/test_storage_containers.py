"""Controls for the absent-vs-empty container scenario.

The battery check asserts one equality against a constant, which is exactly the shape that
can pass for the wrong reason. These drive the scenario against containers in deliberately
wrong states and pin that each is reported as itself — and, at the end, that the oracle's
new bucket concept is bounded the way the real stores bound theirs: reads never create,
writes always do.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from uuid import uuid4

import pytest

from forze.application.contracts.storage import UploadedObject
from forze.base.exceptions import CoreException, exc
from forze_dst.conformance.storage import (
    EXPECTED_CONTAINER_OUTCOME,
    ContainerVerdict,
    run_container_probes,
)
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.state import MockState

# ----------------------- #


def _store() -> MockState:
    return MockState()


def _port(state: MockState, bucket: str) -> MockStorageAdapter:
    return MockStorageAdapter(state=state, bucket=bucket)


async def _one_chunk(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def _emptied(state: MockState, bucket: str) -> MockStorageAdapter:
    """A container that exists and holds nothing — provisioned by a write, then cleared."""

    port = _port(state, bucket)
    stored = await port.upload(UploadedObject(filename="seed.txt", data=b"x"))
    await port.delete(stored.key)

    return port


# ....................... #


async def test_the_oracle_now_tells_absent_from_emptied() -> None:
    state = _store()

    outcome = await run_container_probes(
        absent=_port(state, "never-made"),
        emptied=await _emptied(state, "made-then-cleared"),
        missing_key="nope",
    )

    assert outcome == EXPECTED_CONTAINER_OUTCOME


async def test_a_populated_container_is_not_reported_as_empty() -> None:
    """The verdict distinguishes "answered with nothing" from "answered with something".

    Without this, an oracle that lost every object would look identical to one correctly
    reporting an emptied bucket.
    """

    state = _store()
    port = _port(state, "populated")
    await port.upload(UploadedObject(filename="a.txt", data=b"x"))

    outcome = await run_container_probes(
        absent=_port(state, "never-made"),
        emptied=port,
        missing_key="nope",
    )

    assert outcome.emptied_strict is ContainerVerdict.NON_EMPTY
    assert outcome != EXPECTED_CONTAINER_OUTCOME


async def test_an_unexpected_failure_is_not_swallowed_into_a_verdict() -> None:
    """Only not-found and infrastructure are classified; anything else propagates.

    A scenario that mapped every refusal onto one of its verdicts would report a
    permission error, a validation error or an outright bug as a well-behaved container
    state — and the comparison would then agree across backends for the worst reason.
    """

    class _Broken:
        async def list(self, limit: int, offset: int, *, missing_ok: bool = False) -> tuple:
            raise exc.precondition("not a container problem")

        async def head(self, key: str) -> object:
            raise AssertionError("unreachable")  # pragma: no cover

    state = _store()

    with pytest.raises(CoreException):
        await run_container_probes(
            absent=_Broken(),
            emptied=await _emptied(state, "fine"),
            missing_key="nope",
        )


# ....................... #


async def test_reads_never_provision_a_container() -> None:
    """The boundary the real stores draw, now drawn by the oracle too.

    A read that created the bucket would make *absent* indistinguishable from *empty*
    again — and would silently undo a deletion for any caller that merely listed,
    including the sweep whose guard is exactly that distinction.
    """

    state = _store()
    bucket = f"reads-{uuid4().hex[:8]}"
    port = _port(state, bucket)

    with pytest.raises(CoreException):
        await port.list(10, 0)

    assert await port.list(10, 0, missing_ok=True) == ([], 0)

    with pytest.raises(CoreException):
        await port.head("nope")

    await port.delete("nope")

    # Every one of those touched the container. None of them may have created it.
    with pytest.raises(CoreException):
        await port.list(10, 0)


@pytest.mark.parametrize("write", ["upload", "upload_stream", "presign_upload", "begin_upload"])
async def test_every_documented_write_path_provisions(write: str) -> None:
    """The other half: the four write paths the adapter documents as creating on demand.

    Pinned per path rather than once, because the state they share is easy to reach from
    one of them and forget in another — and a write path that did not provision would
    leave its own object unlistable.
    """

    state = _store()
    port = _port(state, f"writes-{uuid4().hex[:8]}")

    match write:
        case "upload":
            await port.upload(UploadedObject(filename="a.txt", data=b"x"))
        case "upload_stream":
            await port.upload_stream(_one_chunk(b"x"), filename="a.txt")
        case "presign_upload":
            await port.presign_upload("k", expires_in=timedelta(hours=1))
        case "begin_upload":
            await port.begin_upload("k")

    listed, _total = await port.list(10, 0)

    assert isinstance(listed, list), "the container must now exist and be listable"
