"""Postgres invocation-deadline push-down: the per-tx statement_timeout computation."""

from __future__ import annotations

from datetime import timedelta

from forze_postgres.adapters.txmanager import _statement_timeout_ms
from forze_postgres.kernel.client.value_objects import DeadlinePushdownPolicy

# ----------------------- #


class TestStatementTimeoutMs:
    def test_none_when_push_down_disabled(self) -> None:
        # policy None = kill switch off → no statement_timeout applied.
        assert _statement_timeout_ms(None, 5.0) is None

    def test_none_when_no_deadline_bound(self) -> None:
        assert _statement_timeout_ms(DeadlinePushdownPolicy(), None) is None

    def test_budget_ms_without_a_static_cap(self) -> None:
        assert _statement_timeout_ms(DeadlinePushdownPolicy(), 5.1) == 5100

    def test_tighten_only_static_cap_wins_when_tighter(self) -> None:
        policy = DeadlinePushdownPolicy(statement_timeout_cap=timedelta(seconds=2))
        assert _statement_timeout_ms(policy, 5.1) == 2000  # min(2000, 5100)

    def test_budget_wins_when_tighter_than_cap(self) -> None:
        policy = DeadlinePushdownPolicy(statement_timeout_cap=timedelta(seconds=30))
        assert _statement_timeout_ms(policy, 1.1) == 1100  # min(1100, 30000)

    def test_floored_to_positive(self) -> None:
        # A statement_timeout of 0 means *unlimited*, so a ~0 budget floors to >= 1 ms.
        assert _statement_timeout_ms(DeadlinePushdownPolicy(), 0.0) == 1
