"""Mongo computed null-ordering: stage construction, the opt-in gate, and routing.

The pipeline-shape and find-vs-aggregate routing are exercised here with a mocked client;
end-to-end correctness against the oracle lives in the integration suite.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze.domain.models import Document
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.gateways import MongoReadGateway
from tests.unit._gateway_codec_helpers import codec_for

pytestmark = pytest.mark.unit


class _Row(Document):
    score: int | None = None
    seq: int = 0


def _gw(**kw: Any) -> tuple[MongoReadGateway[_Row], MagicMock]:
    client = MagicMock(spec=MongoClient)
    client.collection = AsyncMock(return_value=object())
    client.find_many = AsyncMock(return_value=[])
    client.aggregate = AsyncMock(return_value=[])

    gw = MongoReadGateway(
        relation=("db", "t"),
        client=client,
        model_type=_Row,
        codec=codec_for(_Row),
        tenant_aware=False,
        **kw,
    )
    return gw, client


_OVERRIDE = {"score": {"dir": "asc", "nulls": "last"}, "seq": "asc"}


class TestStages:
    def test_none_when_flag_off(self) -> None:
        gw, _ = _gw(computed_null_ordering=False)
        assert gw.offset_null_sort_stages(_OVERRIDE) is None

    def test_none_when_all_default_nulls(self) -> None:
        gw, _ = _gw(computed_null_ordering=True)
        # canonical placement → native sort suffices
        assert gw.offset_null_sort_stages({"score": "asc", "seq": "asc"}) is None

    def test_builds_rank_for_override(self) -> None:
        gw, _ = _gw(computed_null_ordering=True)
        result = gw.offset_null_sort_stages(_OVERRIDE)

        assert result is not None
        stages, rank_fields = result
        add_fields, sort = stages[0]["$addFields"], stages[1]["$sort"]

        # one rank field for the overridden 'score' key, none for 'seq'
        assert rank_fields == ["__fz_nullrank_0"]
        # nulls LAST → null ranks 1, non-null 0 (so nulls sort after on ascending rank)
        cond = add_fields["__fz_nullrank_0"]["$cond"]
        assert cond[1] == 1 and cond[2] == 0
        # sort: rank asc, then field asc, then seq asc (order preserved)
        assert list(sort.items()) == [
            ("__fz_nullrank_0", 1),
            ("score", 1),
            ("seq", 1),
        ]


class TestGate:
    def test_render_sorts_rejects_override_without_flag(self) -> None:
        gw, _ = _gw(computed_null_ordering=False)
        with pytest.raises(CoreException, match="does not support"):
            gw.render_sorts(_OVERRIDE)

    def test_render_sorts_allows_override_with_flag(self) -> None:
        gw, _ = _gw(computed_null_ordering=True)
        # No raise; the offset path handles the override via the aggregation stages.
        assert gw.render_sorts(_OVERRIDE) == [("score", 1), ("seq", 1)]


class TestRouting:
    @pytest.mark.asyncio
    async def test_override_routes_to_aggregate(self) -> None:
        gw, client = _gw(computed_null_ordering=True)
        await gw.find_many(None, sorts=_OVERRIDE)

        client.aggregate.assert_awaited_once()
        client.find_many.assert_not_awaited()
        # the rank field is projected out
        pipeline = client.aggregate.await_args.args[1]
        assert any("$addFields" in s for s in pipeline)
        assert pipeline[-1]["$project"] == {"__fz_nullrank_0": 0}

    @pytest.mark.asyncio
    async def test_default_sort_routes_to_find(self) -> None:
        gw, client = _gw(computed_null_ordering=True)
        await gw.find_many(None, sorts={"score": "asc", "seq": "asc"})

        client.find_many.assert_awaited_once()
        client.aggregate.assert_not_awaited()
