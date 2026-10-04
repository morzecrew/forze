"""Micro-benchmark for the federated RRF merge on the full-fetch path.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``, no Docker::

    just perf tests/perf/test_forze_federated_merge_perf.py

The merge keys each hit by ``(member, id)``; keying it by its serialized record instead was about
half of a full-path federated search's CPU. ``record_keys`` measures that former keying for
comparison.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.integrations.search.snapshot import SearchResultSnapshot

_MEMBERS = 11
_HITS_PER_MEMBER = 2_000


class _Row(BaseModel):
    id: UUID
    number_id: int
    name: str
    display_name: str
    description: str
    status: str
    price: Decimal
    tags: list[str]
    category_id: UUID
    created_at: datetime
    last_update_at: datetime
    is_deleted: bool


def _legs() -> list[tuple[str, list[BaseModel], float]]:
    now = datetime.now(tz=UTC)

    return [
        (
            f"member_{m}",
            [
                _Row(
                    id=uuid4(),
                    number_id=i,
                    name=f"plata Плата управления двигателем {i}",
                    display_name=f"Плата {i}",
                    description=f"Используется в сборке шасси, партия {i}",
                    status="active",
                    price=Decimal(i) * Decimal("1.25"),
                    tags=["электроника", "плата"],
                    category_id=uuid4(),
                    created_at=now,
                    last_update_at=now,
                    is_deleted=False,
                )
                for i in range(_HITS_PER_MEMBER)
            ],
            1.0,
        )
        for m in range(_MEMBERS)
    ]


@pytest.mark.perf
@pytest.mark.parametrize("keying", ["merge_keys", "record_keys"])
def test_federated_rrf_merge_benchmark(
    benchmark: Any, keying: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``weighted_rrf_merge_rows`` over 11 legs of 2,000 hits."""

    if keying == "record_keys":
        monkeypatch.setattr(
            SearchResultSnapshot,
            "federated_merge_key",
            staticmethod(SearchResultSnapshot.federated_record_key_string),
        )

    legs = _legs()

    result = benchmark(lambda: SearchResultSnapshot.weighted_rrf_merge_rows(leg_rows=legs, k=60))

    assert len(result) == _MEMBERS * _HITS_PER_MEMBER
