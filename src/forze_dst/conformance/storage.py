"""The storage differential — telling an **absent** container from an **empty** one.

Most of a blob store's contract is about objects. This scenario is about the container,
because that is where the oracle was not merely inaccurate but *structurally incapable*: the
mock reached its bucket through ``setdefault``, so the bucket existed the moment anything
asked about it, and ``list(missing_ok=False)`` — a contract this codebase introduced
deliberately, and which the re-encryption sweep depends on — could not fail. Not "did not
fail in these tests": could not. Every mock-backed test of it was vacuously green, and no
amount of test-writing against that oracle would have found it.

That is the difference between this leg and the others. Elsewhere a differential compares
two implementations that both model the thing; here it forced the oracle to *grow* the
concept (:attr:`~forze_mock.MockState.storage_buckets`) before there was anything to
compare, and then pinned it.

The four probes, and why these four:

- an **absent** bucket listed with ``missing_ok=False`` — must raise;
- the same bucket with ``missing_ok=True`` — must read as empty *and say the container was
  absent*. Without the first half the strict probe is satisfied by a backend that simply
  always raises; without the second, ``missing_ok`` would answer a question by deleting it,
  which under per-tenant buckets is the difference between an unprovisioned tenant and an
  idle one;
- an **emptied** bucket (created, written, cleared) — must read as empty with
  ``missing_ok=False``. This is the positive control that makes "raises" mean *absent*
  rather than "raises whenever there are no objects";
- a ``head`` of a missing key, in an existing bucket and in an absent one — which must give
  the **same** answer. Measured, not assumed: MinIO, floci and GCS all return a plain
  not-found either way, because ``HeadObject`` cannot distinguish them. A leg asserting a
  distinction there would have been asserting something no backend does.
"""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from enum import StrEnum
from typing import Any, Protocol

import attrs

from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


class ContainerVerdict(StrEnum):
    """What a read of a container-scoped operation did, normalised across backends."""

    EMPTY = "empty"
    """Answered, with nothing in it — and the container was there to answer for."""

    EMPTY_UNPROVISIONED = "empty-unprovisioned"
    """Answered with nothing *and* said the container does not exist.

    Distinct from :attr:`EMPTY` on purpose. ``missing_ok`` lets a caller tolerate an absent
    container; it must not make the two indistinguishable, or a per-tenant deployment
    reports the same blank page for a tenant nobody provisioned and a tenant that has
    uploaded nothing."""

    NON_EMPTY = "non-empty"

    MISSING_CONTAINER = "missing-container"
    """Refused because the container is not there — the ``infrastructure`` class every
    shipped backend uses for a missing bucket."""

    MISSING_OBJECT = "missing-object"
    """Refused because the object is not there — ``not_found``."""


# ....................... #


class ObjectPage(Protocol):
    """The part of a listing result this scenario reads."""

    @property
    def objects(self) -> Sequence[Any]: ...  # pragma: no cover

    @property
    def container_missing(self) -> bool: ...  # pragma: no cover


class ListsObjects(Protocol):
    """The one read this scenario needs from a storage query port."""

    def list(
        self,
        limit: int,
        offset: int,
        *,
        missing_ok: bool = False,
    ) -> Awaitable[ObjectPage]: ...  # pragma: no cover

    def head(self, key: str) -> Awaitable[Any]: ...  # pragma: no cover


# ....................... #


@attrs.frozen(kw_only=True)
class ContainerOutcome:
    """One verdict per container-scoped probe — the comparable surface of a blob store."""

    absent_strict: ContainerVerdict
    """``list(missing_ok=False)`` over a bucket nothing ever created."""

    absent_tolerant: ContainerVerdict
    """The same list with ``missing_ok=True`` — the caller opting out of the *raise*.

    Must come back ``EMPTY_UNPROVISIONED``, not ``EMPTY``: tolerating the absence is not
    the same as being told nothing about it."""

    emptied_strict: ContainerVerdict
    """``list(missing_ok=False)`` over a bucket that exists and holds nothing.

    The positive control, twice over. Without it, ``absent_strict`` raising proves only
    that something raised, which a backend that refuses every empty listing would satisfy
    just as well — and it is what stops a backend from passing ``absent_tolerant`` by
    flagging *every* empty page as unprovisioned.
    """

    head_absent_object: ContainerVerdict
    """A missing key in a bucket that exists."""

    head_absent_container: ContainerVerdict
    """A missing key in a bucket that does not.

    Expected to equal :attr:`head_absent_object`: no shipped backend distinguishes them at
    this verb, so neither may the oracle.
    """


EXPECTED_CONTAINER_OUTCOME = ContainerOutcome(
    absent_strict=ContainerVerdict.MISSING_CONTAINER,
    absent_tolerant=ContainerVerdict.EMPTY_UNPROVISIONED,
    emptied_strict=ContainerVerdict.EMPTY,
    head_absent_object=ContainerVerdict.MISSING_OBJECT,
    head_absent_container=ContainerVerdict.MISSING_OBJECT,
)
"""What every backend — and now the oracle — must answer."""


# ----------------------- #


async def _list_verdict(port: ListsObjects, *, missing_ok: bool) -> ContainerVerdict:
    try:
        page = await port.list(100, 0, missing_ok=missing_ok)
    except CoreException as error:
        return _refusal_verdict(error)

    if page.container_missing:
        return ContainerVerdict.EMPTY_UNPROVISIONED

    return ContainerVerdict.EMPTY if not page.objects else ContainerVerdict.NON_EMPTY


async def _head_verdict(port: ListsObjects, key: str) -> ContainerVerdict:
    try:
        await port.head(key)
    except CoreException as error:
        return _refusal_verdict(error)

    return ContainerVerdict.NON_EMPTY


def _refusal_verdict(error: CoreException) -> ContainerVerdict:
    """Classify a refusal by its kind, which is the only part backends agree on.

    Messages differ ("S3 bucket not found" / "GCS resource not found") and there is no
    shared code, so the kind is what a portable caller can branch on — and matching on
    anything finer would make this scenario a test of error text.
    """

    if error.kind is ExceptionKind.NOT_FOUND:
        return ContainerVerdict.MISSING_OBJECT

    if error.kind is ExceptionKind.INFRASTRUCTURE:
        return ContainerVerdict.MISSING_CONTAINER

    raise error


# ....................... #


async def run_container_probes(
    *,
    absent: ListsObjects,
    emptied: ListsObjects,
    missing_key: str,
) -> ContainerOutcome:
    """Drive the absent-vs-empty probes and return the verdicts.

    *absent* must be a port over a container nothing has ever written to, and *emptied* one
    over a container that exists and currently holds no objects. The caller owns both
    because provisioning is a wiring fact — on a real store the bucket is created by a
    write or by the deployment, not by this scenario.
    """

    return ContainerOutcome(
        absent_strict=await _list_verdict(absent, missing_ok=False),
        absent_tolerant=await _list_verdict(absent, missing_ok=True),
        emptied_strict=await _list_verdict(emptied, missing_ok=False),
        head_absent_object=await _head_verdict(emptied, missing_key),
        head_absent_container=await _head_verdict(absent, missing_key),
    )
