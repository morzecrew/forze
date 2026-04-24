"""Hub / federated search options strip result-snapshot options from leg options."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import (
    FederatedSearchSpec,
    HubSearchSpec,
    SearchOptions,
    SearchSpec,
)
from forze_postgres.adapters.search._options import (
    prepare_federated_search_options,
    prepare_hub_search_options,
)

# ----


class _Leg(BaseModel):
    x: str


class _HubRow(BaseModel):
    id: str


def _dummy_leg(name: str) -> SearchSpec[Any]:
    return SearchSpec(name=name, model_type=_Leg, fields=["x"])


@pytest.mark.unit
def test_prepare_hub_search_options_strips_result_snapshot_for_legs() -> None:
    hub = HubSearchSpec(
        name="hub",
        model_type=_HubRow,
        members=[_dummy_leg("a"), _dummy_leg("b")],
    )
    opts: SearchOptions = {
        "result_snapshot": {
            "mode": True,
            "id": "run-1",
            "ttl_seconds": 120,
            "max_ids": 10_000,
            "chunk_size": 100,
            "fingerprint": "fp",
        },
        "member_weights": {"a": 1.0, "b": 0.5},
    }
    leg_opts, weights = prepare_hub_search_options(hub, opts)
    assert weights == [1.0, 0.5]
    assert "result_snapshot" not in leg_opts
    assert leg_opts == {}


@pytest.mark.unit
def test_prepare_federated_search_options_strips_result_snapshot_for_legs() -> None:
    fed = FederatedSearchSpec(
        name="fed",
        members=[_dummy_leg("a"), _dummy_leg("b")],
    )
    opts: SearchOptions = {
        "result_snapshot": {
            "mode": "auto",
            "id": "snap",
        },
        "member_weights": {"a": 1.0, "b": 1.0},
    }
    leg_opts, weights = prepare_federated_search_options(fed, opts)
    assert weights == [1.0, 1.0]
    assert "result_snapshot" not in leg_opts
