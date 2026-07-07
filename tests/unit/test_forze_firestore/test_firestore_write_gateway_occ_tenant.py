"""Unit tests for Firestore write-gateway OCC (rev-CAS) and tenant-scoped deletes.

These drive the real :class:`FirestoreWriteGateway` / :class:`FirestoreReadGateway`
against a small in-memory fake client that models Firestore's optimistic concurrency
(a transaction commit aborts when a document read in the transaction changed
underneath it). That lets a lost-update be provoked deterministically without the
emulator: a competing writer mutates the document between the gateway's read and its
commit; the commit aborts, ``@occ_retry`` re-reads, and the concurrent field is
preserved instead of being clobbered.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime
from typing import Any, AsyncGenerator, Callable, Mapping
from uuid import UUID, uuid4

import attrs
import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.kernel.gateways import FirestoreReadGateway, FirestoreWriteGateway
from tests.unit._gateway_codec_helpers import write_codecs_for

pytestmark = pytest.mark.unit


# ----------------------- #
# Models


class _Doc(Document):
    name: str
    note: str = ""


class _Create(CreateDocumentCmd):
    name: str
    note: str = ""


class _Update(BaseDTO):
    name: str | None = None
    note: str | None = None


_DOMAIN_CODEC, _CREATE_CODEC, _UPDATE_CODEC = write_codecs_for(
    domain_type=_Doc,
    create_type=_Create,
    update_type=_Update,
)


# ----------------------- #
# In-memory fake Firestore client with transaction OCC


_DELETE = object()


@attrs.define(slots=True)
class _FakeFirestore:
    """Minimal in-memory Firestore client modelling document-level OCC.

    Each stored document carries a monotonically increasing version (Firestore's
    ``update_time`` analogue). A transaction records the version of every document it
    reads and, on commit, aborts with a CONCURRENCY error if any of them changed —
    exactly the guarantee the write gateway relies on for its rev-CAS.
    """

    store: dict[tuple[str, str], list[Any]] = attrs.field(factory=dict)
    _depth: int = 0
    _reads: dict[tuple[str, str], int] = attrs.field(factory=dict)
    _writes: dict[tuple[str, str], Any] = attrs.field(factory=dict)
    after_read: Callable[[], None] | None = None
    commits: int = 0
    aborts: int = 0

    async def collection(self, name: str, *, database: str | None = None) -> str:
        _ = database
        return name

    def is_in_transaction(self) -> bool:
        return self._depth > 0

    def require_transaction(self) -> None:
        if self._depth == 0:
            raise exc.internal("Transactional context is required")

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        if self._depth > 0:
            self._depth += 1
            try:
                yield None
            finally:
                self._depth -= 1
            return

        self._depth = 1
        self._reads = {}
        self._writes = {}

        try:
            yield None
        except BaseException:
            self._reset_tx()
            raise
        else:
            self._commit()
        finally:
            self._depth = 0

    def _reset_tx(self) -> None:
        self._reads = {}
        self._writes = {}

    def _commit(self) -> None:
        for key, seen in self._reads.items():
            current = self.store.get(key)
            current_version = current[0] if current else 0
            if current_version != seen:
                self._reset_tx()
                self.aborts += 1
                raise CoreException.concurrency("Firestore transaction conflict")

        for key, data in self._writes.items():
            if data is _DELETE:
                self.store.pop(key, None)
                continue
            prev = self.store.get(key)
            version = (prev[0] if prev else 0) + 1
            self.store[key] = [version, dict(data)]

        self.commits += 1
        self._reset_tx()

    def _snapshot(self, key: tuple[str, str]) -> dict[str, Any] | None:
        if self._depth > 0 and key in self._writes:
            data = self._writes[key]
            return None if data is _DELETE else dict(data)
        current = self.store.get(key)
        return dict(current[1]) if current else None

    async def get_document(self, coll: str, doc_id: str) -> dict[str, Any] | None:
        key = (coll, doc_id)
        snap = self._snapshot(key)

        if self._depth > 0:
            current = self.store.get(key)
            self._reads.setdefault(key, current[0] if current else 0)

        result: dict[str, Any] | None = None
        if snap is not None:
            snap["id"] = doc_id
            result = snap

        if self.after_read is not None:
            hook = self.after_read
            self.after_read = None
            hook()

        return result

    async def set_document(
        self,
        coll: str,
        doc_id: str,
        data: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        _ = merge
        key = (coll, doc_id)
        payload = dict(data)

        if self._depth > 0:
            self._writes[key] = payload
            return

        prev = self.store.get(key)
        version = (prev[0] if prev else 0) + 1
        self.store[key] = [version, payload]

    async def create_document(
        self,
        coll: str,
        doc_id: str,
        data: Mapping[str, Any],
    ) -> None:
        """Create-only write: raise ``conflict`` when the id already exists.

        Models Firestore's ``create`` (ALREADY_EXISTS → ``conflict``) as opposed
        to ``set``'s upsert, so a create that would clobber an existing document
        fails closed instead.
        """

        key = (coll, doc_id)

        if self._snapshot(key) is not None:
            raise CoreException.conflict("Document already exists.")

        payload = dict(data)

        if self._depth > 0:
            self._writes[key] = payload
            return

        prev = self.store.get(key)
        version = (prev[0] if prev else 0) + 1
        self.store[key] = [version, payload]

    async def insert_many(
        self,
        coll: str,
        documents: list[tuple[str, Mapping[str, Any]]],
        *,
        batch_size: int = 200,
        create_only: bool = False,
    ) -> None:
        _ = batch_size

        if create_only:
            # Firestore's WriteBatch.commit() is all-or-nothing: validate the whole batch for
            # conflicts *before* applying any write, so a later conflict can't leave earlier
            # documents created.
            for doc_id, _data in documents:
                if self._snapshot((coll, doc_id)) is not None:
                    raise CoreException.conflict("Document already exists.")
            for doc_id, data in documents:
                await self.create_document(coll, doc_id, data)
        else:
            for doc_id, data in documents:
                await self.set_document(coll, doc_id, data)

    async def delete_document(self, coll: str, doc_id: str) -> None:
        key = (coll, doc_id)

        if self._depth > 0:
            self._writes[key] = _DELETE
            return

        self.store.pop(key, None)

    # test helpers -------------------------------------------------------- #

    def external_write(self, coll: str, doc_id: str, patch: Mapping[str, Any]) -> None:
        """Commit a competing writer's change out-of-band (bumps the version)."""

        key = (coll, doc_id)
        prev = self.store.get(key)
        base = dict(prev[1]) if prev else {}
        base.update(patch)
        version = (prev[0] if prev else 0) + 1
        self.store[key] = [version, base]

    def seed(self, coll: str, doc_id: str, data: Mapping[str, Any]) -> None:
        self.store[(coll, doc_id)] = [1, dict(data)]


# ----------------------- #
# Gateway construction


_RELATION = ("(default)", "docs")


def _gateways(
    client: _FakeFirestore,
    *,
    tenant_aware: bool = False,
    tenant: UUID | None = None,
) -> FirestoreWriteGateway[_Doc, _Create, _Update]:
    provider = (lambda: TenantIdentity(tenant_id=tenant)) if tenant is not None else None

    read = FirestoreReadGateway(
        relation=_RELATION,
        client=client,
        model_type=_Doc,
        codec=_DOMAIN_CODEC,
        tenant_aware=tenant_aware,
        tenant_provider=provider,
    )
    return FirestoreWriteGateway(
        relation=_RELATION,
        client=client,
        model_type=_Doc,
        codec=_DOMAIN_CODEC,
        create_cmd_type=_Create,
        update_cmd_type=_Update,
        read_gw=read,
        create_codec=_CREATE_CODEC,
        update_codec=_UPDATE_CODEC,
        tenant_aware=tenant_aware,
        tenant_provider=provider,
    )


# ----------------------- #
# BUG 1 — rev-CAS lost-update prevention


@pytest.mark.asyncio
async def test_update_is_conditional_and_preserves_concurrent_field() -> None:
    """A writer that read rev N must not clobber a concurrent, non-overlapping change.

    The competing writer fires between this update's read and its commit, changing a
    different field (``note``). The unconditional full-document write would clobber
    ``note``; the transactional rev-CAS aborts the first commit, ``@occ_retry``
    re-reads, and the final document keeps *both* changes.
    """

    client = _FakeFirestore()
    write = _gateways(client)

    created = await write.create(_Create(name="A", note="orig"))
    pk = created.id
    key = ("docs", str(pk))

    # Competing committed writer, injected right after this update reads the document.
    client.after_read = lambda: client.external_write(
        "docs", str(pk), {"note": "concurrent"}
    )

    updated, _ = await write.update(pk, _Update(name="B"))

    assert client.aborts == 1, "first commit must abort on the concurrent change"
    assert updated.name == "B"  # our change applied
    assert updated.note == "concurrent"  # concurrent change NOT clobbered
    # rev advanced past the concurrent writer's version, not just base+1.
    assert client.store[key][1]["name"] == "B"
    assert client.store[key][1]["note"] == "concurrent"


@pytest.mark.asyncio
async def test_update_without_contention_commits_once() -> None:
    client = _FakeFirestore()
    write = _gateways(client)

    created = await write.create(_Create(name="A"))
    baseline_commits = client.commits

    updated, _ = await write.update(created.id, _Update(name="B"))

    assert client.aborts == 0
    assert client.commits == baseline_commits + 1
    assert updated.name == "B"
    assert updated.rev == created.rev + 1


# ----------------------- #
# BUG 2 — tenant-scoped kill


def _kind(err: CoreException) -> ExceptionKind:
    return err.kind


@pytest.mark.asyncio
async def test_kill_rejects_cross_tenant_document() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    client = _FakeFirestore()
    write = _gateways(client, tenant_aware=True, tenant=tenant_a)

    # A document owned by tenant B, present in the shared (tagged) collection.
    other_id = uuid4()
    other_key = ("docs", str(other_id))
    client.seed(
        "docs",
        str(other_id),
        {
            "id": str(other_id),
            "rev": 1,
            "name": "theirs",
            TENANT_ID_FIELD: str(tenant_b),
        },
    )

    with pytest.raises(CoreException) as ei:
        await write.kill(other_id)

    assert _kind(ei.value) is ExceptionKind.NOT_FOUND
    assert other_key in client.store, "cross-tenant document must not be deleted"


@pytest.mark.asyncio
async def test_kill_deletes_own_tenant_document() -> None:
    tenant_a = uuid4()
    client = _FakeFirestore()
    write = _gateways(client, tenant_aware=True, tenant=tenant_a)

    created = await write.create(_Create(name="mine"))
    key = ("docs", str(created.id))
    assert key in client.store

    await write.kill(created.id)

    assert key not in client.store


@pytest.mark.asyncio
async def test_kill_missing_document_raises_not_found() -> None:
    client = _FakeFirestore()
    write = _gateways(client, tenant_aware=True, tenant=uuid4())

    with pytest.raises(CoreException) as ei:
        await write.kill(uuid4())

    assert _kind(ei.value) is ExceptionKind.NOT_FOUND


@pytest.mark.asyncio
async def test_kill_many_does_not_touch_cross_tenant_document() -> None:
    tenant_a = uuid4()
    tenant_b = uuid4()
    client = _FakeFirestore()
    write = _gateways(client, tenant_aware=True, tenant=tenant_a)

    other_id = uuid4()
    client.seed(
        "docs",
        str(other_id),
        {"id": str(other_id), "rev": 1, "name": "theirs", TENANT_ID_FIELD: str(tenant_b)},
    )

    with pytest.raises(CoreException) as ei:
        await write.kill_many([other_id])

    assert _kind(ei.value) is ExceptionKind.NOT_FOUND
    assert ("docs", str(other_id)) in client.store


# ----------------------- #
# BUG 3 — create fails closed instead of silently overwriting


@pytest.mark.asyncio
async def test_create_rejects_existing_id() -> None:
    """``create`` with an id that already exists conflicts, matching PG/Mongo —
    it must not silently overwrite the existing document."""

    client = _FakeFirestore()
    write = _gateways(client)

    first = await write.create(_Create(name="original"), id=uuid4())
    key = ("docs", str(first.id))

    with pytest.raises(CoreException) as ei:
        await write.create(_Create(name="clobber"), id=first.id)

    assert _kind(ei.value) is ExceptionKind.CONFLICT
    assert client.store[key][1]["name"] == "original"  # untouched


@pytest.mark.asyncio
async def test_ensure_returns_existing_without_overwriting() -> None:
    """``ensure`` on an already-present id returns it unchanged (no overwrite)."""

    client = _FakeFirestore()
    write = _gateways(client)

    first = await write.create(_Create(name="original"), id=uuid4())
    key = ("docs", str(first.id))
    version_before = client.store[key][0]

    got = await write.ensure(first.id, _Create(name="would-overwrite"))

    assert got.id == first.id
    assert got.name == "original"
    assert client.store[key][1]["name"] == "original"
    assert client.store[key][0] == version_before  # no write happened


@pytest.mark.asyncio
async def test_ensure_wins_create_race_returns_existing() -> None:
    """A concurrent writer creates the id between ensure's read and its create.

    The now fail-closed create conflicts; ensure catches it, re-reads, and
    returns the winner's document instead of surfacing the conflict.
    """

    client = _FakeFirestore()
    write = _gateways(client)

    target = uuid4()
    key = ("docs", str(target))

    # After ensure's initial (missing) read, a competitor commits the document.
    client.after_read = lambda: client.seed(
        "docs", str(target), {"id": str(target), "rev": 1, "name": "winner"}
    )

    got = await write.ensure(target, _Create(name="loser"))

    assert got.name == "winner"  # existing row, not our clobbering create
    assert client.store[key][1]["name"] == "winner"


@pytest.mark.asyncio
async def test_upsert_wins_create_race_updates_existing() -> None:
    """When upsert loses the create race, it updates the now-existing row."""

    client = _FakeFirestore()
    write = _gateways(client)

    target = uuid4()
    key = ("docs", str(target))

    client.after_read = lambda: client.seed(
        "docs", str(target), {"id": str(target), "rev": 1, "name": "winner"}
    )

    got = await write.upsert(target, _Create(name="loser"), _Update(note="patched"))

    assert got.note == "patched"  # update applied to the raced-in row
    assert client.store[key][1]["name"] == "winner"
