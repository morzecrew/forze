"""Adaptive cache lifetimes: age-proportional per-entry TTL + spec validation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.cache import AgeBasedTtl, CacheSpec
from forze.application.integrations.document import DocumentCache
from forze.base.exceptions import CoreException
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #

_PK = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_PK2 = UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")


class DocModel(BaseModel):
    id: UUID
    rev: int
    last_update_at: datetime
    payload: str = ""


class BareModel(BaseModel):
    id: UUID
    rev: int


_CODEC = codec_for(DocModel)


def _doc(pk: UUID = _PK, *, age: timedelta) -> DocModel:
    return DocModel(
        id=pk,
        rev=1,
        last_update_at=datetime.now(timezone.utc) - age,
    )


def _spec(**kw: object) -> CacheSpec:
    params: dict[str, object] = {
        "name": "c",
        "ttl": timedelta(seconds=300),
        "age_ttl": AgeBasedTtl(
            alpha=0.1,
            min_ttl=timedelta(seconds=30),
            max_ttl=timedelta(hours=1),
        ),
    }
    params.update(kw)
    return CacheSpec(**params)  # type: ignore[arg-type]


def _coord(cache: AsyncMock, spec: CacheSpec | None = None) -> DocumentCache[DocModel]:
    return DocumentCache(
        read_model_type=DocModel,
        read_codec=_CODEC,
        document_name="widgets",
        cache=cache,
        after_commit=None,
        cache_spec=spec if spec is not None else _spec(),
    )


# ----------------------- #


class TestSpecValidation:
    def test_age_ttl_rejects_invalid(self) -> None:
        for kw in (
            {"alpha": 0.0},
            {"min_ttl": timedelta(0)},
            {"min_ttl": timedelta(hours=2), "max_ttl": timedelta(hours=1)},
        ):
            with pytest.raises(CoreException):
                AgeBasedTtl(**kw)  # type: ignore[arg-type]

    def test_sliding_ttl_rejects_invalid(self) -> None:
        for kw in (
            {"sliding_ttl": timedelta(0)},
            {"sliding_ttl": timedelta(seconds=300)},  # >= ttl
        ):
            with pytest.raises(CoreException):
                CacheSpec(name="c", ttl=timedelta(seconds=300), **kw)  # type: ignore[arg-type]

    def test_valid_knobs_accepted(self) -> None:
        spec = CacheSpec(
            name="c",
            ttl=timedelta(seconds=300),
            sliding_ttl=timedelta(seconds=60),
            age_ttl=AgeBasedTtl(),
        )

        assert spec.sliding_ttl == timedelta(seconds=60)
        assert spec.age_ttl is not None


class TestAgeProportionalTtl:
    async def test_old_document_earns_long_ttl(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache)

        await coord.set_one(_doc(age=timedelta(hours=5)))  # 10% of 5h = 30min

        (_, kwargs) = cache.set_versioned.await_args
        ttl = kwargs["ttl"]
        # Quantized (ceil, two significant digits), so allow a coarse band.
        assert timedelta(minutes=29) <= ttl <= timedelta(minutes=32)

    async def test_fresh_document_clamped_to_floor(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache)

        await coord.set_one(_doc(age=timedelta(seconds=10)))  # 10% of 10s << floor

        (_, kwargs) = cache.set_versioned.await_args
        assert kwargs["ttl"] == timedelta(seconds=30)

    async def test_ancient_document_clamped_to_cap(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache)

        await coord.set_one(_doc(age=timedelta(days=30)))

        (_, kwargs) = cache.set_versioned.await_args
        assert kwargs["ttl"] == timedelta(hours=1)

    async def test_without_opt_in_ttl_is_none(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache, spec=CacheSpec(name="c", ttl=timedelta(seconds=300)))

        await coord.set_one(_doc(age=timedelta(hours=5)))

        (_, kwargs) = cache.set_versioned.await_args
        assert kwargs["ttl"] is None

    async def test_model_without_last_update_at_falls_back(self) -> None:
        cache = AsyncMock()
        coord = DocumentCache(
            read_model_type=BareModel,
            read_codec=codec_for(BareModel),
            document_name="widgets",
            cache=cache,
            after_commit=None,
            cache_spec=_spec(),
        )

        await coord.set_one(BareModel(id=_PK, rev=1))

        (_, kwargs) = cache.set_versioned.await_args
        assert kwargs["ttl"] is None

    async def test_set_many_groups_by_quantized_ttl(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache)

        # Two near-identical ages quantize into one bucket; one ancient doc
        # lands in the cap bucket.
        docs = [
            _doc(_PK, age=timedelta(hours=5)),
            _doc(_PK2, age=timedelta(hours=5, seconds=20)),
            _doc(UUID(int=7), age=timedelta(days=30)),
        ]

        await coord.set_many(docs)

        assert cache.set_many_versioned.await_count == 2
        batch_ttls = {
            call.kwargs["ttl"] for call in cache.set_many_versioned.await_args_list
        }
        assert timedelta(hours=1) in batch_ttls  # the cap bucket

    async def test_envelope_carries_entry_ttl_for_xfetch(self) -> None:
        cache = AsyncMock()
        coord = _coord(cache, spec=_spec(early_refresh_beta=1.0))

        await coord.set_one(_doc(age=timedelta(hours=5)), delta=0.5)

        ((_, _, payload), kwargs) = cache.set_versioned.await_args
        assert payload["_xf"]["ttl"] == pytest.approx(
            kwargs["ttl"].total_seconds()
        )
