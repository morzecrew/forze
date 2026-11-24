"""``no_permission_outside_its_tenant`` over hand-built histories.

The end-to-end workload lives in ``test_forze_authz/test_tenant_role_bindings_dst.py``; this
pins what the check reads from a history.
"""

from __future__ import annotations

import pytest

from forze_dst.invariants import check, no_permission_outside_its_tenant
from forze_dst.oracle.recorder import Event, History

pytestmark = pytest.mark.unit

# ----------------------- #

INVARIANT = no_permission_outside_its_tenant(["write_in_u"])


def _op(op: str, **fields: object) -> Event:
    return Event(seq=0, kind="operation", at=0.0, fields={"op": op, **fields})


def _history(*events: Event) -> History:
    return History(seed=0, events=events)


class TestOverAHistory:
    def test_a_success_outside_the_tenant_is_flagged(self) -> None:
        violations = check(_history(_op("write_in_u", outcome="ok")), [INVARIANT])

        assert [v.invariant for v in violations] == ["no_permission_outside_its_tenant"]

    def test_a_refusal_outside_the_tenant_holds(self) -> None:
        assert check(_history(_op("write_in_u", outcome="raised")), [INVARIANT]) == []

    def test_an_operation_it_was_not_given_is_not_judged(self) -> None:
        assert check(_history(_op("write_in_t", outcome="ok")), [INVARIANT]) == []

    def test_an_event_with_no_outcome_reads_as_a_success(self) -> None:
        # record_operation records only an operation that returned, and stamps no outcome.
        violations = check(_history(_op("write_in_u", result=None)), [INVARIANT])

        assert len(violations) == 1


def test_it_needs_an_operation() -> None:
    with pytest.raises(ValueError, match="at least one operation"):
        no_permission_outside_its_tenant([])
