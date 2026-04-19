"""Federated search: RRF merge and PostgresFederatedSearchAdapter behavior."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchReadModel,
    FederatedSearchSpec,
    SearchSpec,
)
from forze.base.errors import CoreError
from forze_postgres.adapters.search.federated import (
    PostgresFederatedSearchAdapter,
    weighted_rrf_merge_rows,
)
from forze_postgres.execution.deps.configs import validate_postgres_federated_search_conf

# ----------------------- #


class _Hit(BaseModel):
    id: int
    label: str = ""


def _mem(name: str) -> SearchSpec[_Hit]:
    return SearchSpec(name=name, model_type=_Hit, fields=["label"])


def _fed() -> FederatedSearchSpec[_Hit]:
    return FederatedSearchSpec(
        name="fed",
        members=(_mem("a"), _mem("b")),
    )


def test_weighted_rrf_merge_applies_branch_weights() -> None:
    x = _Hit(id=1, label="x")
    y = _Hit(id=2, label="y")
    k = 60
    merged = weighted_rrf_merge_rows(
        leg_rows=(
            ("a", (x, y), 2.0),
            ("b", (y, x), 1.0),
        ),
        k=k,
    )
    scores = {m.hit.id: sc for m, sc in merged}
    # y ranks 2 on a (2/(60+2)) and 1 on b (1/(60+1)); x is the reverse.
    assert scores[2] > scores[1]


def test_weighted_rrf_skips_non_positive_weight_leg() -> None:
    only = weighted_rrf_merge_rows(
        leg_rows=(
            ("a", (_Hit(id=1),), 0.0),
            ("b", (_Hit(id=2),), 1.0),
        ),
        k=60,
    )
    assert len(only) == 1
    assert only[0][0].hit.id == 2
    assert only[0][0].member == "b"


def test_validate_postgres_federated_search_conf_requires_two_members() -> None:
    with pytest.raises(CoreError, match="at least two"):
        validate_postgres_federated_search_conf(
            {
                "members": {
                    "a": {
                        "index": ("public", "i"),
                        "read": ("public", "r"),
                        "engine": "pgroonga",
                    },
                },
            },
        )


@pytest.mark.asyncio
async def test_federated_search_skips_zero_weight_members() -> None:
    pa = MagicMock()
    pa.search = AsyncMock(return_value=([_Hit(id=1)], 1))
    pb = MagicMock()
    pb.search = AsyncMock(return_value=([_Hit(id=2)], 1))
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=50,
    )
    out, total = await adapter.search(
        "q",
        options={"member_weights": {"a": 0.0, "b": 1.0}},
    )
    pa.search.assert_not_awaited()
    pb.search.assert_awaited_once()
    assert total == 1
    assert len(out) == 1
    assert isinstance(out[0], FederatedSearchReadModel)
    assert out[0].member == "b"
    assert out[0].hit.id == 2


@pytest.mark.asyncio
async def test_federated_search_pagination_on_merged_pool() -> None:
    async def leg_a(*_a, **_kw):
        return [_Hit(id=i) for i in range(3)], 3

    async def leg_b(*_a, **_kw):
        return [_Hit(id=i + 10) for i in range(3)], 3

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=leg_a)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=leg_b)
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_k=60,
        rrf_per_leg_limit=10,
    )
    out, total = await adapter.search("q", pagination={"offset": 2, "limit": 2})
    assert total == 6
    assert len(out) == 2


@pytest.mark.asyncio
async def test_federated_search_all_members_disabled_returns_empty() -> None:
    na = MagicMock()
    na.search = AsyncMock()
    nb = MagicMock()
    nb.search = AsyncMock()
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", na), ("b", nb)),
    )
    out, total = await adapter.search("q", options={"members": []})
    assert out == []
    assert total == 0


def test_federated_adapter_rejects_leg_count_mismatch() -> None:
    with pytest.raises(CoreError, match="match.*members length"):
        PostgresFederatedSearchAdapter(
            federated_spec=_fed(),
            legs=(("a", MagicMock()),),
        )


def test_federated_adapter_rejects_leg_name_mismatch() -> None:
    pa = MagicMock()
    pb = MagicMock()
    with pytest.raises(CoreError, match="does not match SearchSpec.name"):
        PostgresFederatedSearchAdapter(
            federated_spec=_fed(),
            legs=(("wrong", pa), ("b", pb)),
        )


@pytest.mark.asyncio
async def test_federated_search_rejects_return_fields() -> None:
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", MagicMock()), ("b", MagicMock())),
    )
    with pytest.raises(CoreError, match="return_fields"):
        await adapter.search("q", return_fields=("id",))


class _FedRow(BaseModel):
    hit: _Hit
    member: str


@pytest.mark.asyncio
async def test_federated_search_return_type_validates_rows() -> None:
    h = _Hit(id=1, label="a")

    async def one_hit(*_a, **_kw):
        return [h], 1

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=one_hit)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=one_hit)
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
    )
    out, total = await adapter.search("q", return_type=_FedRow)
    assert total >= 1
    assert len(out) >= 1
    assert isinstance(out[0], _FedRow)
    assert out[0].member in {"a", "b"}
    assert out[0].hit.id == 1


@pytest.mark.asyncio
async def test_federated_search_applies_sorts_on_merged_field() -> None:
    async def leg_a(*_a, **_kw):
        return [_Hit(id=1, label="b"), _Hit(id=2, label="a")], 2

    async def leg_b(*_a, **_kw):
        return [_Hit(id=3, label="c")], 1

    pa = MagicMock()
    pa.search = AsyncMock(side_effect=leg_a)
    pb = MagicMock()
    pb.search = AsyncMock(side_effect=leg_b)
    adapter = PostgresFederatedSearchAdapter(
        federated_spec=_fed(),
        legs=(("a", pa), ("b", pb)),
        rrf_per_leg_limit=10,
    )
    out, total = await adapter.search(
        "q",
        sorts={"label": "asc"},
        pagination={"offset": 0, "limit": 10},
    )
    assert total == 3
    assert len(out) == 3
    assert {row.hit.id for row in out} == {1, 2, 3}
