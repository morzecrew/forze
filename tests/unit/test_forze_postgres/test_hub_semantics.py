"""Unit tests for canonical hub combo semantics."""

from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search.hub.constants import HUB_RANK
from forze_postgres.adapters.search.hub.runtime import HubLegRuntime
from forze_postgres.adapters.search.hub.semantics import (
    aggregate_rank,
    hub_order_key_spec,
    leg_contribution,
    merge_hub_combo_rows,
    passes_combine,
    sql_leg_coalesce,
    sql_leg_matched,
)

# ----------------------- #


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


def test_leg_contribution_single_fk() -> None:
    leg = _leg(fk="fk")
    fk = uuid4()
    raw, matched = leg_contribution(leg, {"fk": fk}, {fk: 0.75})
    assert raw == 0.75
    assert matched is True


def test_leg_contribution_multi_fk_max_branch() -> None:
    leg = _leg(fk=("fk_a", "fk_b"))
    a, b = uuid4(), uuid4()
    raw, matched = leg_contribution(
        leg,
        {"fk_a": a, "fk_b": b},
        {a: 0.2, b: 0.9},
    )
    assert raw == 0.9
    assert matched is True


def test_aggregate_rank_max_and_sum() -> None:
    assert aggregate_rank([0.1, 0.8], "max") == 0.8
    assert aggregate_rank([0.1, 0.8], "sum") == 0.9


def test_passes_combine_or_and() -> None:
    assert passes_combine([True, False], "or") is True
    assert passes_combine([True, False], "and") is False


def test_merge_hub_combo_rows_sum_and() -> None:
    hub_id = uuid4()
    fk = uuid4()
    leg = _leg(fk="fk")
    out = merge_hub_combo_rows(
        hub_rows=[{"id": hub_id, "fk": fk, "label": "y"}],
        leg_ranked=[(leg, {fk: 1.0})],
        weights=[1.0],
        score_merge="sum",
        combine="and",
        read_fields=frozenset({"id", "fk", "label"}),
        rank_field=HUB_RANK,
    )
    assert len(out) == 1
    assert out[0][HUB_RANK] == 1.0


def test_hub_order_key_spec_ranked_includes_rank() -> None:
    spec = hub_order_key_spec(
        do_legs=True,
        sorts=None,
        default_sort=None,
        read_fields=frozenset({"id"}),
        spec_name="hub",
        rank_field=HUB_RANK,
    )
    assert spec[0] == (HUB_RANK, "desc")


def test_sql_emitters_single_fk_shape() -> None:
    leg = _leg(fk="fk")
    coalesce = sql_leg_coalesce(leg, 0)
    matched = sql_leg_matched(leg, 0)
    coalesce_s = coalesce.as_string(None)
    matched_s = matched.as_string(None)
    assert "lp0" in coalesce_s
    assert "IS NOT NULL" in matched_s
