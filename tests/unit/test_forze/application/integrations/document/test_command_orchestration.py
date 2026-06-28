"""Pure unit tests for document command + query orchestration.

These exercise :class:`DocumentCommandMixin` and :class:`DocumentQueryMixin`
directly through lightweight in-memory fakes (no Docker, no ``MockState``). The
fakes only implement the handful of gateway / cache methods the mixins actually
invoke and return canned rows / counts so we can drive the chunked
``update_matching_strict`` loop, the fast ``update_matching`` path, the bulk
command edge branches and the scattered get/find/select/stream query branches
through their cap/limit/empty/return-shape variants.

The ``update_matching_strict`` harness composes the real query mixin behind the
command mixin (mirroring the production MRO) so ``project_many`` and
``update_many`` run for real on top of the fake gateways.
"""

from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import KeyedCreate, KeyedUpdate, UpsertItem
from forze.application.integrations.document._command import DocumentCommandMixin
from forze.application.integrations.document._query import DocumentQueryMixin
from forze.base.exceptions import CoreException

# ----------------------- #

_UID_A = UUID("00000000-0000-0000-0000-0000000000aa")
_UID_B = UUID("00000000-0000-0000-0000-0000000000bb")
_UID_C = UUID("00000000-0000-0000-0000-0000000000cc")


class _Row(BaseModel):
    """Minimal read model carrying the ``id`` / ``rev`` versioned fields."""

    id: str
    rev: int = 1


class _Domain(BaseModel):
    """Minimal domain row returned by the write gateway."""

    id: UUID
    rev: int = 1


class _Dto:
    """Bare create/update payload exposing the ``id`` ``require_create_id`` reads."""

    def __init__(self, id: UUID | None = None) -> None:  # noqa: A002
        self.id = id


# ....................... #


class FakeCache:
    """No-op stand-in for ``DocumentCache`` recording invalidation / set calls."""

    def __init__(self, *, id_rev_capable: bool = True) -> None:
        self._id_rev_capable = id_rev_capable
        self.invalidated: list[UUID] = []
        self.set_one_calls: list[Any] = []
        self.set_many_calls: list[Any] = []

    def id_rev_capable(self) -> bool:
        return self._id_rev_capable

    def read_through_eligible(self, *, skip_cache: bool, return_fields: Any) -> bool:
        return False

    async def invalidate_keys_now(self, *pks: UUID) -> None:
        self.invalidated.extend(pks)

    async def after_commit_or_now(self, fn: Any) -> None:
        await fn()

    async def set_one(self, doc: Any) -> None:
        self.set_one_calls.append(doc)

    async def set_many(self, docs: Any) -> None:
        self.set_many_calls.append(docs)


# ....................... #


class FakeWriteGateway:
    """In-memory stand-in for ``DocumentWriteGatewayPort``.

    Records calls and returns staged domain rows / counts. Only the methods the
    command mixin invokes are implemented.
    """

    def __init__(
        self,
        *,
        create_result: Any = None,
        create_many_result: Sequence[Any] | None = None,
        ensure_result: Any = None,
        ensure_many_result: Sequence[Any] | None = None,
        upsert_result: Any = None,
        upsert_many_result: Sequence[Any] | None = None,
        update_matching_result: tuple[int, Sequence[Any]] | None = None,
        update_many_result: tuple[Sequence[Any], Sequence[Any]] | None = None,
    ) -> None:
        self._create_result = create_result
        self._create_many_result = list(create_many_result or [])
        self._ensure_result = ensure_result
        self._ensure_many_result = list(ensure_many_result or [])
        self._upsert_result = upsert_result
        self._upsert_many_result = list(upsert_many_result or [])
        self._update_matching_result = update_matching_result or (0, [])
        self._update_many_result = update_many_result
        self.calls: list[str] = []

    async def create(self, payload: Any, *, id: Any = None) -> Any:
        self.calls.append("create")
        return self._create_result

    async def create_many(self, payloads: Any, *, batch_size: int) -> Sequence[Any]:
        self.calls.append("create_many")
        return self._create_many_result

    async def ensure(self, id: Any, payload: Any) -> Any:
        self.calls.append("ensure")
        return self._ensure_result

    async def ensure_many(
        self, ids: Any, payloads: Any, *, batch_size: int
    ) -> Sequence[Any]:
        self.calls.append("ensure_many")
        return self._ensure_many_result

    async def upsert(self, id: Any, create: Any, update: Any) -> Any:
        self.calls.append("upsert")
        return self._upsert_result

    async def upsert_many(
        self, ids: Any, creates: Any, updates: Any, *, batch_size: int
    ) -> Sequence[Any]:
        self.calls.append("upsert_many")
        return self._upsert_many_result

    async def update(self, pk: Any, dto: Any, *, rev: int) -> tuple[Any, Any]:
        self.calls.append("update")
        return _Domain(id=pk, rev=rev + 1), {"rev": rev + 1}

    async def touch(self, pk: Any) -> Any:
        self.calls.append("touch")
        return _Domain(id=pk, rev=2)

    async def touch_many(self, pks: Any, *, batch_size: int) -> Sequence[Any]:
        self.calls.append("touch_many")
        return [_Domain(id=p, rev=2) for p in pks]

    async def kill(self, pk: Any) -> None:
        self.calls.append("kill")

    async def kill_many(self, pks: Any, *, batch_size: int) -> None:
        self.calls.append("kill_many")

    async def update_matching(
        self,
        filters: Any,
        dto: Any,
        *,
        batch_size: int,
    ) -> tuple[int, Sequence[Any]]:
        self.calls.append("update_matching")
        return self._update_matching_result

    async def update_many(
        self,
        pks: Any,
        dtos: Any,
        *,
        revs: Any,
        batch_size: int,
    ) -> tuple[Sequence[Any], Sequence[Any]]:
        self.calls.append("update_many")
        if self._update_many_result is not None:
            return self._update_many_result
        domains = [_Domain(id=p, rev=r + 1) for p, r in zip(pks, revs, strict=True)]
        diffs = [{"rev": r + 1} for r in revs]
        return domains, diffs


# ....................... #


class FakeProjectReadGateway:
    """Read gateway returning staged ``project_many`` pages for the strict loop.

    ``update_matching_strict`` reaches the read side through ``project_many`` →
    ``_offset_page`` → ``read_gw.find_many``. With a single windowed fetch
    (``pagination={"limit": ...}``) the offset path issues exactly one
    ``find_many`` per chunk, so each staged page maps to one strict chunk.
    """

    def __init__(self, *, pages: list[list[dict[str, Any]]] | None = None) -> None:
        self._pages = list(pages or [])
        self.find_many_calls: list[dict[str, Any]] = []

    def compile_filters(self, filters: Any) -> Any:
        return ("parsed", filters)

    async def find_many(self, **kwargs: Any) -> list[Any]:
        self.find_many_calls.append(kwargs)
        if self._pages:
            return self._pages.pop(0)
        return []


# ....................... #


class CommandHarness(DocumentCommandMixin[_Row, _Domain, _Dto, _Dto]):
    """Concrete command mixin host wiring the abstract hooks to simple fakes."""

    def __init__(
        self,
        write_gw: FakeWriteGateway,
        cache: FakeCache | None = None,
        *,
        eff_batch_size: int = 50,
    ) -> None:
        self.write_gw = write_gw  # type: ignore[assignment]
        self.document_cache = cache or FakeCache()  # type: ignore[assignment]
        self._eff_batch_size = eff_batch_size

        class _Spec:
            name = "thing"

        self.spec = _Spec()  # type: ignore[assignment]

    @property
    def eff_batch_size(self) -> int:
        return self._eff_batch_size

    def _require_write(self) -> Any:
        if self.write_gw is None:
            raise CoreException("no write gateway")
        return self.write_gw

    async def _to_read(self, domain: Any, *, pk: Any = None) -> _Row:
        return _Row(id=str(domain.id), rev=domain.rev)

    async def _to_read_many(self, domains: Any, *, pks: Any = None) -> Sequence[_Row]:
        return [_Row(id=str(d.id), rev=d.rev) for d in domains]

    async def _finalize_single_write(
        self,
        domain: Any,
        *,
        return_new: bool,
        pk: Any = None,
    ) -> _Row | None:
        if not return_new:
            return None
        return _Row(id=str(domain.id), rev=domain.rev)

    async def _finalize_bulk_write(
        self,
        domains: Any,
        *,
        return_new: bool,
        pks: Any = None,
    ) -> Sequence[_Row] | None:
        if not return_new:
            return None
        return [_Row(id=str(d.id), rev=d.rev) for d in domains]


# ....................... #


class StrictHarness(
    DocumentCommandMixin[_Row, _Domain, _Dto, _Dto],
    DocumentQueryMixin[_Row],
):
    """Command + query host for the real chunked ``update_matching_strict`` loop.

    Mirrors the production MRO (command in front, query behind) so ``project_many``
    and ``update_many`` execute against the fake gateways.
    """

    def __init__(
        self,
        read_gw: FakeProjectReadGateway,
        write_gw: FakeWriteGateway,
        *,
        eff_batch_size: int = 2,
        max_chunked_command_pages: int | None = None,
    ) -> None:
        self.read_gw = read_gw  # type: ignore[assignment]
        self.write_gw = write_gw  # type: ignore[assignment]
        self.document_cache = FakeCache()  # type: ignore[assignment]
        self._eff_batch_size = eff_batch_size
        self.max_chunked_command_pages = max_chunked_command_pages
        self.enforce_primary_key_cursor_sort = False

        class _Spec:
            name = "thing"

        self.spec = _Spec()  # type: ignore[assignment]

    @property
    def _read_fields(self) -> frozenset[str]:
        return frozenset({"id", "rev"})

    @property
    def eff_batch_size(self) -> int:
        return self._eff_batch_size

    @property
    def max_scan_pages(self) -> int | None:
        return None

    @property
    def max_stream_pages(self) -> int | None:
        return None

    def _eff_stream_chunk_size(self, chunk_size: int) -> int:
        return chunk_size

    def _resolve_sorts(self, sorts: Any) -> Any:
        return sorts if sorts else {"id": "asc"}

    def _require_write(self) -> Any:
        return self.write_gw

    async def _to_read_many(self, domains: Any, *, pks: Any = None) -> Sequence[_Row]:
        return [_Row(id=str(d.id), rev=d.rev) for d in domains]


# ----------------------- #
# update_matching (fast path)


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_update_matching_return_shapes(return_new: bool) -> None:
    domains = [_Domain(id=_UID_A), _Domain(id=_UID_B)]
    write_gw = FakeWriteGateway(update_matching_result=(2, domains))
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.update_matching({"k": "v"}, _Dto(), return_new=return_new)

    if return_new:
        assert [r.id for r in res] == [str(_UID_A), str(_UID_B)]
    else:
        assert res == 2

    # matched pks were invalidated
    assert cache.invalidated == [_UID_A, _UID_B]
    assert "update_matching" in write_gw.calls


# ....................... #


@pytest.mark.asyncio
async def test_update_matching_empty_match_skips_invalidate() -> None:
    write_gw = FakeWriteGateway(update_matching_result=(0, []))
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.update_matching({"k": "v"}, _Dto(), return_new=False)

    assert res == 0
    # no pks -> invalidate_keys_now not called
    assert cache.invalidated == []


# ....................... #


@pytest.mark.asyncio
async def test_update_matching_return_new_finalize_none_raises() -> None:
    # A finalize that returns None despite return_new=True must surface internal.
    write_gw = FakeWriteGateway(update_matching_result=(1, [_Domain(id=_UID_A)]))
    harness = CommandHarness(write_gw)

    async def _finalize_none(domains: Any, *, return_new: bool, pks: Any = None) -> None:
        return None

    harness._finalize_bulk_write = _finalize_none  # type: ignore[method-assign]

    with pytest.raises(CoreException, match="Failed to finalize bulk write"):
        await harness.update_matching({"k": "v"}, _Dto(), return_new=True)


# ----------------------- #
# update_matching_strict (chunked project_many -> update_many loop)


@pytest.mark.asyncio
async def test_update_matching_strict_rejects_nonpositive_chunk() -> None:
    harness = StrictHarness(FakeProjectReadGateway(), FakeWriteGateway())

    with pytest.raises(CoreException, match="chunk_size must be positive"):
        await harness.update_matching_strict({"k": "v"}, _Dto(), chunk_size=0)


# ....................... #


@pytest.mark.asyncio
async def test_update_matching_strict_empty_first_page() -> None:
    read_gw = FakeProjectReadGateway(pages=[[]])
    write_gw = FakeWriteGateway()
    harness = StrictHarness(read_gw, write_gw, eff_batch_size=2)

    res = await harness.update_matching_strict({"k": "v"}, _Dto(), return_new=False)

    assert res == 0
    # broke before any update_many
    assert "update_many" not in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_update_matching_strict_chunked_loop(return_new: bool) -> None:
    # chunk size 2: a full page (2 rows) then a short page (1 row) terminates.
    read_gw = FakeProjectReadGateway(
        pages=[
            [
                {"id": str(_UID_A), "rev": 1},
                {"id": str(_UID_B), "rev": 1},
            ],
            [
                {"id": str(_UID_C), "rev": 2},
            ],
        ],
    )
    write_gw = FakeWriteGateway()
    harness = StrictHarness(read_gw, write_gw, eff_batch_size=2)

    res = await harness.update_matching_strict(
        {"k": "v"},
        _Dto(),
        return_new=return_new,
    )

    if return_new:
        assert [r.id for r in res] == [str(_UID_A), str(_UID_B), str(_UID_C)]
    else:
        assert res == 3

    # two chunks -> two update_many invocations
    assert write_gw.calls.count("update_many") == 2
    # second projection chunk carried a keyset $gt on the last id of chunk 1
    assert len(read_gw.find_many_calls) == 2


# ....................... #


@pytest.mark.asyncio
async def test_update_matching_strict_single_short_page() -> None:
    # one page shorter than chunk -> single iteration, no keyset follow-up fetch.
    read_gw = FakeProjectReadGateway(
        pages=[[{"id": str(_UID_A), "rev": 1}]],
    )
    write_gw = FakeWriteGateway()
    harness = StrictHarness(read_gw, write_gw, eff_batch_size=5)

    res = await harness.update_matching_strict({"k": "v"}, _Dto(), return_new=False)

    assert res == 1
    assert len(read_gw.find_many_calls) == 1


# ....................... #


@pytest.mark.asyncio
async def test_update_matching_strict_respects_max_chunked_pages() -> None:
    # Every page is full so the loop only stops at the page cap.
    read_gw = FakeProjectReadGateway(
        pages=[
            [{"id": str(_UID_A), "rev": 1}, {"id": str(_UID_B), "rev": 1}],
            [{"id": str(_UID_C), "rev": 1}, {"id": str(_UID_A), "rev": 1}],
            [{"id": str(_UID_B), "rev": 1}, {"id": str(_UID_C), "rev": 1}],
        ],
    )
    write_gw = FakeWriteGateway()
    harness = StrictHarness(
        read_gw,
        write_gw,
        eff_batch_size=2,
        max_chunked_command_pages=1,
    )

    with pytest.raises(CoreException, match="max_pages=1"):
        await harness.update_matching_strict({"k": "v"}, _Dto(), return_new=False)


# ----------------------- #
# bulk command edge branches (empty-input short circuits)


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_create_many_empty(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    harness = CommandHarness(write_gw)

    res = await harness.create_many([], return_new=return_new)

    assert res == ([] if return_new else None)
    assert "create_many" not in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_ensure_many_empty(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    harness = CommandHarness(write_gw)

    res = await harness.ensure_many([], return_new=return_new)

    assert res == ([] if return_new else None)
    assert "ensure_many" not in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_upsert_many_empty(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    harness = CommandHarness(write_gw)

    res = await harness.upsert_many([], return_new=return_new)

    assert res == ([] if return_new else None)
    assert "upsert_many" not in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_create_populates_cache(return_new: bool) -> None:
    write_gw = FakeWriteGateway(create_result=_Domain(id=_UID_A))
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.create(_Dto(), return_new=return_new)

    assert (res is None) is (not return_new)
    assert cache.invalidated == [_UID_A]


# ....................... #


@pytest.mark.asyncio
async def test_ensure_invalidates_and_returns() -> None:
    write_gw = FakeWriteGateway(ensure_result=_Domain(id=_UID_A))
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.ensure(_UID_A, _Dto())

    assert res is not None and res.id == str(_UID_A)
    assert cache.invalidated == [_UID_A]


# ....................... #


@pytest.mark.asyncio
async def test_upsert_invalidates_and_returns() -> None:
    write_gw = FakeWriteGateway(upsert_result=_Domain(id=_UID_A))
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.upsert(_UID_A, _Dto(), _Dto())

    assert res is not None and res.id == str(_UID_A)
    assert cache.invalidated == [_UID_A]


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_create_many_populates_cache(return_new: bool) -> None:
    domains = [_Domain(id=_UID_A), _Domain(id=_UID_B)]
    write_gw = FakeWriteGateway(create_many_result=domains)
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.create_many([_Dto(), _Dto()], return_new=return_new)

    if return_new:
        assert [r.id for r in res] == [str(_UID_A), str(_UID_B)]
    else:
        assert res is None
    assert cache.invalidated == [_UID_A, _UID_B]


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_ensure_many_populates_cache(return_new: bool) -> None:
    domains = [_Domain(id=_UID_A), _Domain(id=_UID_B)]
    write_gw = FakeWriteGateway(ensure_many_result=domains)
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.ensure_many(
        [KeyedCreate(id=_UID_A, payload=_Dto()), KeyedCreate(id=_UID_B, payload=_Dto())],
        return_new=return_new,
    )

    if return_new:
        assert [r.id for r in res] == [str(_UID_A), str(_UID_B)]
    else:
        assert res is None
    assert cache.invalidated == [_UID_A, _UID_B]
    assert "ensure_many" in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_upsert_many_populates_cache(return_new: bool) -> None:
    domains = [_Domain(id=_UID_A), _Domain(id=_UID_B)]
    write_gw = FakeWriteGateway(upsert_many_result=domains)
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.upsert_many(
        [
            UpsertItem(id=_UID_A, create=_Dto(), update=_Dto()),
            UpsertItem(id=_UID_B, create=_Dto(), update=_Dto()),
        ],
        return_new=return_new,
    )

    if return_new:
        assert [r.id for r in res] == [str(_UID_A), str(_UID_B)]
    else:
        assert res is None
    assert cache.invalidated == [_UID_A, _UID_B]
    assert "upsert_many" in write_gw.calls


# ----------------------- #
# single / bulk update + touch + kill paths


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "return_new,return_diff",
    [(True, False), (True, True), (False, False), (False, True)],
)
async def test_update_return_shapes(return_new: bool, return_diff: bool) -> None:
    write_gw = FakeWriteGateway()
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.update(
        _UID_A,
        1,
        _Dto(),
        return_new=return_new,
        return_diff=return_diff,
    )

    if return_new and return_diff:
        row, diff = res
        assert row.id == str(_UID_A) and diff == {"rev": 2}
        assert cache.set_one_calls
    elif return_new:
        assert res.id == str(_UID_A)
        assert cache.set_one_calls
    elif return_diff:
        assert res == {"rev": 2}
    else:
        assert res is None
    assert cache.invalidated == [_UID_A]


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "return_new,return_diff",
    [(True, False), (True, True), (False, False), (False, True)],
)
async def test_update_many_return_shapes(return_new: bool, return_diff: bool) -> None:
    write_gw = FakeWriteGateway()
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.update_many(
        [
            KeyedUpdate(id=_UID_A, rev=1, dto=_Dto()),
            KeyedUpdate(id=_UID_B, rev=2, dto=_Dto()),
        ],
        return_new=return_new,
        return_diff=return_diff,
    )

    if return_new and return_diff:
        assert [row.id for row, _ in res] == [str(_UID_A), str(_UID_B)]
        assert cache.set_many_calls
    elif return_new:
        assert [row.id for row in res] == [str(_UID_A), str(_UID_B)]
        assert cache.set_many_calls
    elif return_diff:
        assert res == [{"rev": 2}, {"rev": 3}]
    else:
        assert res is None
    assert cache.invalidated == [_UID_A, _UID_B]


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_update_many_empty(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    harness = CommandHarness(write_gw)

    res = await harness.update_many([], return_new=return_new)

    assert res == ([] if return_new else None)
    assert "update_many" not in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_touch_return_shapes(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.touch(_UID_A, return_new=return_new)

    assert (res is None) is (not return_new)
    assert cache.invalidated == [_UID_A]
    assert "touch" in write_gw.calls


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_touch_many_return_shapes(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    res = await harness.touch_many([_UID_A, _UID_B], return_new=return_new)

    if return_new:
        assert [r.id for r in res] == [str(_UID_A), str(_UID_B)]
    else:
        assert res is None
    assert cache.invalidated == [_UID_A, _UID_B]


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize("return_new", [True, False])
async def test_touch_many_empty(return_new: bool) -> None:
    write_gw = FakeWriteGateway()
    harness = CommandHarness(write_gw)

    res = await harness.touch_many([], return_new=return_new)

    assert res == ([] if return_new else None)
    assert "touch_many" not in write_gw.calls


# ....................... #


@pytest.mark.asyncio
async def test_kill_evicts_cache() -> None:
    write_gw = FakeWriteGateway()
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    await harness.kill(_UID_A)

    assert cache.invalidated == [_UID_A]
    assert "kill" in write_gw.calls


# ....................... #


@pytest.mark.asyncio
async def test_kill_many_evicts_cache() -> None:
    write_gw = FakeWriteGateway()
    cache = FakeCache()
    harness = CommandHarness(write_gw, cache)

    await harness.kill_many([_UID_A, _UID_B])

    assert cache.invalidated == [_UID_A, _UID_B]
    assert "kill_many" in write_gw.calls


# ....................... #


@pytest.mark.asyncio
async def test_kill_many_empty_short_circuits() -> None:
    write_gw = FakeWriteGateway()
    harness = CommandHarness(write_gw)

    await harness.kill_many([])

    assert "kill_many" not in write_gw.calls


# ----------------------- #
# query orchestration edge branches (_query.py)


class QueryHarness(DocumentQueryMixin[_Row]):
    """Concrete query mixin host backed by a fake read gateway + cache."""

    def __init__(
        self,
        read_gw: Any,
        *,
        id_rev_capable: bool = True,
        eff_batch_size: int = 2,
    ) -> None:
        self.read_gw = read_gw  # type: ignore[assignment]
        self.document_cache = FakeCache(id_rev_capable=id_rev_capable)  # type: ignore[assignment]
        self._eff_batch_size = eff_batch_size
        self.enforce_primary_key_cursor_sort = False

    @property
    def _read_fields(self) -> frozenset[str]:
        return frozenset({"id", "rev"})

    @property
    def eff_batch_size(self) -> int:
        return self._eff_batch_size

    @property
    def max_scan_pages(self) -> int | None:
        return None

    @property
    def max_stream_pages(self) -> int | None:
        return None

    def _eff_stream_chunk_size(self, chunk_size: int) -> int:
        return chunk_size

    def _resolve_sorts(self, sorts: Any) -> Any:
        return sorts if sorts else {"id": "asc"}


# ....................... #


class FakeFindGateway:
    """Read gateway covering the single-row find/project/select + stream paths."""

    def __init__(
        self,
        *,
        find_result: Any = None,
        cursor_results: list[list[Any]] | None = None,
        find_many_results: list[list[Any]] | None = None,
        count_value: int = 1,
    ) -> None:
        self.model_type = _Row
        self._find_result = find_result
        self._cursor_results = list(cursor_results or [])
        self._find_many_results = list(find_many_results or [])
        self._count_value = count_value
        self.find_calls: list[dict[str, Any]] = []

    def compile_filters(self, filters: Any) -> Any:
        return ("parsed", filters)

    async def get(self, pk: Any, *, for_update: Any = False) -> Any:
        return self._find_result

    async def get_many(self, pks: Any) -> Any:
        return []

    async def find(self, filters: Any, **kwargs: Any) -> Any:
        self.find_calls.append({"filters": filters, **kwargs})
        return self._find_result

    async def find_many(self, **kwargs: Any) -> list[Any]:
        if self._find_many_results:
            return self._find_many_results.pop(0)
        return []

    async def find_many_aggregates(self, **kwargs: Any) -> list[Any]:
        if self._find_many_results:
            return self._find_many_results.pop(0)
        return []

    async def find_many_with_cursor(self, filters: Any, **kwargs: Any) -> list[Any]:
        if self._cursor_results:
            return self._cursor_results.pop(0)
        return []

    async def count(self, filters: Any, *, parsed: Any = None) -> int:
        return self._count_value

    async def count_aggregates(
        self,
        filters: Any,
        *,
        aggregates: Any,
        parsed: Any = None,
    ) -> int:
        return self._count_value


# ....................... #


@pytest.mark.asyncio
async def test_get_rejects_when_not_id_rev_capable() -> None:
    harness = QueryHarness(FakeFindGateway(), id_rev_capable=False)

    with pytest.raises(CoreException, match="does not have defined id field"):
        await harness.get(_UID_A)


# ....................... #


@pytest.mark.asyncio
async def test_get_many_rejects_when_not_id_rev_capable() -> None:
    harness = QueryHarness(FakeFindGateway(), id_rev_capable=False)

    with pytest.raises(CoreException, match="does not have defined id field"):
        await harness.get_many([_UID_A])


# ....................... #


@pytest.mark.asyncio
async def test_get_many_empty_short_circuits() -> None:
    harness = QueryHarness(FakeFindGateway(), id_rev_capable=False)

    # empty pks returns before the capability check
    assert await harness.get_many([]) == []


# ....................... #


@pytest.mark.asyncio
async def test_get_falls_through_to_gateway_when_cache_ineligible() -> None:
    row = _Row(id="a")
    harness = QueryHarness(FakeFindGateway(find_result=row))

    assert await harness.get(_UID_A) is row


# ....................... #


@pytest.mark.asyncio
async def test_get_many_falls_through_to_gateway_when_cache_ineligible() -> None:
    harness = QueryHarness(FakeFindGateway())

    assert await harness.get_many([_UID_A]) == []


# ....................... #


@pytest.mark.asyncio
async def test_find_delegates_to_gateway() -> None:
    row = _Row(id="a")
    gw = FakeFindGateway(find_result=row)
    harness = QueryHarness(gw)

    assert await harness.find({"k": "v"}) is row
    assert gw.find_calls[0]["for_update"] is False


# ....................... #


@pytest.mark.asyncio
async def test_project_passes_return_fields() -> None:
    gw = FakeFindGateway(find_result={"id": "a"})
    harness = QueryHarness(gw)

    assert await harness.project({"k": "v"}, ["id"]) == {"id": "a"}
    assert gw.find_calls[0]["return_fields"] == ("id",)


# ....................... #


@pytest.mark.asyncio
async def test_select_passes_return_model() -> None:
    row = _Row(id="a")
    gw = FakeFindGateway(find_result=row)
    harness = QueryHarness(gw)

    assert await harness.select({"k": "v"}, _Row) is row
    assert gw.find_calls[0]["return_model"] is _Row


# ....................... #


@pytest.mark.asyncio
async def test_count_delegates_to_gateway() -> None:
    harness = QueryHarness(FakeFindGateway(count_value=4))

    assert await harness.count({"k": "v"}) == 4


# ....................... #


@pytest.mark.asyncio
async def test_select_page_aggregated_returns_page() -> None:
    gw = FakeFindGateway(find_many_results=[[{"id": "a"}]])
    harness = QueryHarness(gw)

    page = await harness.select_page_aggregated(
        _Row,
        {"total": {"$sum": "amount"}},
        pagination={"limit": 5},
    )

    assert hasattr(page, "count")


# ....................... #


@pytest.mark.asyncio
async def test_select_many_aggregated_returns_countless_page() -> None:
    gw = FakeFindGateway(find_many_results=[[{"id": "a"}]])
    harness = QueryHarness(gw)

    page = await harness.select_many_aggregated(
        _Row,
        {"total": {"$sum": "amount"}},
        pagination={"limit": 5},
    )

    assert not hasattr(page, "count")


# ....................... #


@pytest.mark.asyncio
async def test_select_page_returns_page() -> None:
    gw = FakeFindGateway(find_many_results=[[_Row(id="a")]])
    harness = QueryHarness(gw)

    page = await harness.select_page(_Row, pagination={"limit": 5})

    assert hasattr(page, "count")
    assert [h.id for h in page.hits] == ["a"]


# ....................... #


@pytest.mark.asyncio
async def test_select_cursor_returns_cursor_page() -> None:
    gw = FakeFindGateway(cursor_results=[[_Row(id="a")]])
    harness = QueryHarness(gw)

    page = await harness.select_cursor(_Row, cursor={"limit": 5}, sorts={"id": "asc"})

    assert [h.id for h in page.hits] == ["a"]
    assert page.has_more is False


# ....................... #


async def _drain(gen: Any) -> list[Any]:
    chunks = []
    async for chunk in gen:
        chunks.append(chunk)
    return chunks


# ....................... #


@pytest.mark.asyncio
async def test_find_stream_yields_chunks() -> None:
    gw = FakeFindGateway(cursor_results=[[_Row(id="a"), _Row(id="b")]])
    harness = QueryHarness(gw)

    chunks = await _drain(
        harness.find_stream({"k": "v"}, sorts={"id": "asc"}, chunk_size=2)
    )

    assert [r.id for r in chunks[0]] == ["a", "b"]


# ....................... #


@pytest.mark.asyncio
async def test_project_stream_yields_field_chunks() -> None:
    gw = FakeFindGateway(cursor_results=[[{"id": "a"}, {"id": "b"}]])
    harness = QueryHarness(gw)

    chunks = await _drain(
        harness.project_stream(["id"], {"k": "v"}, sorts={"id": "asc"}, chunk_size=2)
    )

    assert chunks[0] == [{"id": "a"}, {"id": "b"}]


# ....................... #


@pytest.mark.asyncio
async def test_select_stream_yields_model_chunks() -> None:
    gw = FakeFindGateway(cursor_results=[[_Row(id="a"), _Row(id="b")]])
    harness = QueryHarness(gw)

    chunks = await _drain(
        harness.select_stream(_Row, {"k": "v"}, sorts={"id": "asc"}, chunk_size=2)
    )

    assert all(isinstance(r, _Row) for r in chunks[0])
