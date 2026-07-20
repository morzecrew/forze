"""Unit tests for parallel hub combo merge and sort."""

from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search.hub.constants import HUB_RANK
from forze_postgres.adapters.search.hub.merge import merge_hub_leg_rows
from forze_postgres.adapters.search.hub.runtime import HubLegRuntime
from forze_postgres.adapters.search.hub.semantics import (
    hub_order_key_spec,
    merge_hub_combo_rows,
    sort_hub_rows,
)


class _HubRow(BaseModel):
    id: str
    fk_a: str
    fk_b: str
    label: str = ""


def _leg(*, fk: str | tuple[str, ...]) -> HubLegRuntime:
    spec = SearchSpec(name="leg", model_type=_HubRow, fields=["label"])
    return HubLegRuntime(
        search=spec,
        index_relation=("public", "idx"),
        index_heap_relation=("public", "heap"),
        hub_fk_columns=fk if isinstance(fk, tuple) else fk,
        heap_pk_column="id",
        engine="pgroonga",
    )


def test_merge_hub_combo_rows_or_max_multi_fk() -> None:
    hub_id = uuid4()
    a, b = uuid4(), uuid4()
    hub_rows = [
        {"id": hub_id, "fk_a": a, "fk_b": b, "label": "x"},
    ]
    leg = _leg(fk=("fk_a", "fk_b"))
    merged = merge_hub_combo_rows(
        hub_rows=hub_rows,
        leg_ranked=[(leg, {a: 0.9, b: 0.2})],
        weights=[1.0],
        score_merge="max",
        combine="or",
        read_fields=frozenset({"id", "fk_a", "fk_b", "label"}),
    )
    assert len(merged) == 1
    assert merged[0][HUB_RANK] == 0.9


def test_merge_hub_combo_rows_and_requires_all_legs() -> None:
    hub_id = uuid4()
    fk = uuid4()
    hub_rows = [{"id": hub_id, "fk": fk, "label": "y"}]
    leg = _leg(fk="fk")
    only = merge_hub_combo_rows(
        hub_rows=hub_rows,
        leg_ranked=[(leg, {fk: 1.0})],
        weights=[1.0],
        score_merge="sum",
        combine="and",
        read_fields=frozenset({"id", "fk", "label"}),
    )
    assert len(only) == 1

    missing = merge_hub_combo_rows(
        hub_rows=hub_rows,
        leg_ranked=[(leg, {})],
        weights=[1.0],
        score_merge="sum",
        combine="and",
        read_fields=frozenset({"id", "fk", "label"}),
    )
    assert missing == []


def test_merge_hub_leg_rows_keeps_higher_rank() -> None:
    id1, id2 = uuid4(), uuid4()
    a = [{HUB_RANK: 0.5, "id": id1}]
    b = [{HUB_RANK: 0.8, "id": id1}, {HUB_RANK: 0.1, "id": id2}]
    out = merge_hub_leg_rows(
        leg_rows=(a, b),
        weights=[1.0, 1.0],
        score_merge="max",
        combine="or",
        read_fields=frozenset({"id"}),
    )
    by_id = {r["id"]: r[HUB_RANK] for r in out}
    assert by_id[id1] == 0.8
    assert by_id[id2] == 0.1


def test_sort_merged_hub_rows_user_sort_then_rank() -> None:
    rows = [
        {HUB_RANK: 0.1, "label": "b"},
        {HUB_RANK: 0.9, "label": "a"},
    ]
    key_spec = hub_order_key_spec(
        do_legs=True,
        sorts={"label": "asc"},  # type: ignore[arg-type]
        default_sort=None,
        read_fields=frozenset({"label"}),
        spec_name="hub",
        rank_field=HUB_RANK,
    )
    sort_hub_rows(rows, key_spec=key_spec)
    assert [r["label"] for r in rows] == ["a", "b"]


def test_sort_merged_hub_rows_id_desc_matches_keyset() -> None:
    u1 = uuid4()
    u2 = uuid4()
    rows = [
        {HUB_RANK: 1.0, "id": u1},
        {HUB_RANK: 1.0, "id": u2},
    ]
    key_spec = hub_order_key_spec(
        do_legs=True,
        sorts={"id": "desc"},  # type: ignore[arg-type]
        default_sort=None,
        read_fields=frozenset({"id"}),
        spec_name="hub",
        rank_field=HUB_RANK,
    )
    sort_hub_rows(rows, key_spec=key_spec)
    assert rows[0]["id"] == max(u1, u2)
    assert rows[1]["id"] == min(u1, u2)
