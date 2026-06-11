"""Unit tests for persistence gateway mixins."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.application.integrations.persistence import (
    FilterParserMixin,
    HistoryOccMixin,
    ModelCodecGatewayMixin,
    TenantResolvedRelationMixin,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import ModelCodec, default_model_codec
from forze.domain.models import Document


class _Model(BaseModel):
    id: UUID
    name: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class _CodecGateway(ModelCodecGatewayMixin[_Model]):
    model_type: type[_Model]
    codec: ModelCodec[_Model, Any]


@attrs.define(slots=True, kw_only=True, frozen=True)
class _FilterGateway(FilterParserMixin):
    filter_limits: QueryFilterLimits | None = None
    filter_parser: QueryFilterExpressionParser = attrs.field(
        default=attrs.Factory(lambda self: self.build_filter_parser(), takes_self=True),
        init=False,
    )


@attrs.define(slots=True, kw_only=True, frozen=True)
class _TenantGateway(TenantResolvedRelationMixin):
    tenant_aware: bool = True
    tenant_provider: Any = None


def test_model_codec_gateway_read_fields_cached() -> None:
    gw = _CodecGateway(
        model_type=_Model,
        codec=default_model_codec(_Model),
    )
    assert gw.read_fields == frozenset({"id", "name"})
    assert gw.read_fields is gw.read_fields


def test_filter_parser_compile_filters_none() -> None:
    gw = _FilterGateway(filter_limits=None)
    assert gw.compile_filters(None) is None


def test_tenant_id_for_resolve_requires_tenant_when_aware() -> None:
    gw = _TenantGateway(tenant_provider=lambda: None)
    with pytest.raises(CoreException, match="Tenant ID is required") as ei:
        gw._tenant_id_for_resolve()
    # Aligned with the ``TenantRequired`` before-hook: a missing tenant on a
    # tenant-aware operation is an authentication failure, not an internal bug.
    assert ei.value.kind == ExceptionKind.AUTHENTICATION
    assert ei.value.code == "tenant_required"


def test_tenant_id_for_resolve_returns_id() -> None:
    tid = uuid4()

    @attrs.define
    class _Tenant:
        tenant_id: UUID

    gw = _TenantGateway(tenant_provider=lambda: _Tenant(tenant_id=tid))
    assert gw._tenant_id_for_resolve() == tid


# ....................... #


class _HistDoc(Document):
    name: str


class _HistDueDoc(Document):
    name: str
    due: datetime


class _FakeHistoryGw:
    """History gateway stub returning a preset record list (any order)."""

    def __init__(self, records: list[_HistDoc]) -> None:
        self.records = records
        self.read_calls: list[tuple[list[UUID], list[int]]] = []

    async def write_many(self, data) -> None:  # pragma: no cover - unused
        pass

    async def read_many(self, pks, revs) -> list[_HistDoc]:
        self.read_calls.append((list(pks), list(revs)))
        return list(self.records)


class _OccGateway(HistoryOccMixin[_HistDoc]):
    def __init__(self, history_gw: _FakeHistoryGw | None) -> None:
        self._history_gw = history_gw

    @property
    def history_gw(self) -> _FakeHistoryGw | None:
        return self._history_gw


def _occ_fixture() -> tuple[
    _HistDoc,
    _HistDoc,
    _HistDoc,
    _HistDoc,
]:
    """Two documents at rev 3 plus their rev-2 history snapshots.

    Names are stable between rev 2 and 3, so an update touching ``name`` is
    consistent when paired with the *correct* history snapshot, but conflicts
    when paired with the other document's snapshot (different name).
    """

    id_a, id_b = uuid4(), uuid4()

    current_a = _HistDoc(id=id_a, rev=3, name="alpha")
    current_b = _HistDoc(id=id_b, rev=3, name="beta")
    hist_a = _HistDoc(id=id_a, rev=2, name="alpha")
    hist_b = _HistDoc(id=id_b, rev=2, name="beta")

    return current_a, current_b, hist_a, hist_b


class TestHistoryOccMixin:
    @pytest.mark.asyncio
    async def test_validate_history_passes_in_request_order(self) -> None:
        current_a, current_b, hist_a, hist_b = _occ_fixture()
        gw = _OccGateway(_FakeHistoryGw([hist_a, hist_b]))

        await gw._validate_history(
            (current_a, 2, {"name": "alpha-updated"}),
            (current_b, 2, {"name": "beta-updated"}),
        )

    @pytest.mark.asyncio
    async def test_validate_history_pairs_correctly_when_backend_shuffles(
        self,
    ) -> None:
        # Regression: backends are not required to return records in request
        # order; positional zip pairing compared the wrong snapshots.
        current_a, current_b, hist_a, hist_b = _occ_fixture()
        gw = _OccGateway(_FakeHistoryGw([hist_b, hist_a]))

        await gw._validate_history(
            (current_a, 2, {"name": "alpha-updated"}),
            (current_b, 2, {"name": "beta-updated"}),
        )

    @pytest.mark.asyncio
    async def test_validate_history_conflict_raises(self) -> None:
        current_a, _, _, _ = _occ_fixture()
        # Historical snapshot whose name already diverged from current: the
        # rev-2 client's name update conflicts with the rev-3 change.
        hist_a_diverged = _HistDoc(id=current_a.id, rev=2, name="old-alpha")
        gw = _OccGateway(_FakeHistoryGw([hist_a_diverged]))

        with pytest.raises(CoreException) as exc_info:
            await gw._validate_history((current_a, 2, {"name": "alpha-updated"}))

        assert exc_info.value.code == "historical_consistency_violation"

    @pytest.mark.asyncio
    async def test_validate_history_missing_record_count_raises_retry(self) -> None:
        current_a, current_b, hist_a, _ = _occ_fixture()
        gw = _OccGateway(_FakeHistoryGw([hist_a]))

        with pytest.raises(CoreException) as exc_info:
            await gw._validate_history(
                (current_a, 2, {"name": "alpha-updated"}),
                (current_b, 2, {"name": "beta-updated"}),
            )

        assert exc_info.value.code == "history_not_found_retry"

    @pytest.mark.asyncio
    async def test_validate_history_wrong_key_raises_retry(self) -> None:
        # Right record count, but one requested (id, rev) is absent.
        current_a, current_b, hist_a, _ = _occ_fixture()
        gw = _OccGateway(_FakeHistoryGw([hist_a, hist_a]))

        with pytest.raises(CoreException) as exc_info:
            await gw._validate_history(
                (current_a, 2, {"name": "alpha-updated"}),
                (current_b, 2, {"name": "beta-updated"}),
            )

        assert exc_info.value.code == "history_not_found_retry"

    @pytest.mark.asyncio
    async def test_validate_history_future_rev_raises_mismatch(self) -> None:
        current_a, _, _, _ = _occ_fixture()
        gw = _OccGateway(_FakeHistoryGw([]))

        with pytest.raises(CoreException) as exc_info:
            await gw._validate_history((current_a, 5, {"name": "x"}))

        assert exc_info.value.code == "revision_mismatch"

    @pytest.mark.asyncio
    async def test_validate_history_without_gateway_rejects_stale_rev(self) -> None:
        current_a, _, _, _ = _occ_fixture()
        gw = _OccGateway(None)

        with pytest.raises(CoreException) as exc_info:
            await gw._validate_history((current_a, 2, {"name": "x"}))

        assert exc_info.value.code == "revision_mismatch"

    @pytest.mark.asyncio
    async def test_validate_history_noop_datetime_resend_passes(self) -> None:
        # Regression: a stale client echoing the identical python datetime it
        # read used to register as a touch against the json-mode historical
        # dump (datetime != ISO string) and raised a false
        # ``historical_consistency_violation`` when another writer had
        # concurrently changed that same field.
        pk = uuid4()
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        hist = _HistDueDoc(id=pk, rev=2, name="alpha", due=due)
        current = _HistDueDoc(
            id=pk,
            rev=3,
            name="alpha",
            due=datetime(2027, 2, 2, tzinfo=timezone.utc),
        )
        gw = _OccGateway(_FakeHistoryGw([hist]))  # type: ignore[list-item]

        await gw._validate_history((current, 2, {"due": due}))

    @pytest.mark.asyncio
    async def test_validate_history_datetime_resend_untouched_field_passes(
        self,
    ) -> None:
        # Echoing identical datetimes for fields no other writer touched is a
        # no-op and must not conflict with concurrent changes elsewhere.
        pk = uuid4()
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        hist = _HistDueDoc(id=pk, rev=2, name="alpha", due=due)
        current = _HistDueDoc(id=pk, rev=3, name="beta", due=due)
        gw = _OccGateway(_FakeHistoryGw([hist]))  # type: ignore[list-item]

        await gw._validate_history((current, 2, {"due": due}))

    @pytest.mark.asyncio
    async def test_validate_history_genuine_datetime_conflict_raises(self) -> None:
        # A *different* datetime for a concurrently-changed field still conflicts.
        pk = uuid4()
        hist = _HistDueDoc(
            id=pk,
            rev=2,
            name="alpha",
            due=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        current = _HistDueDoc(
            id=pk,
            rev=3,
            name="alpha",
            due=datetime(2027, 2, 2, tzinfo=timezone.utc),
        )
        gw = _OccGateway(_FakeHistoryGw([hist]))  # type: ignore[list-item]

        with pytest.raises(CoreException) as exc_info:
            await gw._validate_history(
                (current, 2, {"due": datetime(2028, 3, 3, tzinfo=timezone.utc)})
            )

        assert exc_info.value.code == "historical_consistency_violation"
