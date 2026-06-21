"""Branch coverage for invariant filtering paths over hand-built histories."""

from __future__ import annotations

from forze_dst.oracle.invariants import (
    no_resource_leak,
    read_your_writes,
    single_key_per_operation,
)
from forze_dst.oracle.recorder import Event, History

# ----------------------- #


def _trace(seq: int, **fields: object) -> Event:
    return Event(seq=seq, kind="trace", at=0.0, fields=fields)


# ....................... #


class TestNoResourceLeak:
    def test_surface_filter_and_unrelated_op(self) -> None:
        inv = no_resource_leak(open_op="open", close_op="close", surface="locks")
        history = History(
            seed=0,
            events=(
                _trace(0, surface="other", op="open"),  # filtered out by surface
                _trace(1, surface="locks", op="touch"),  # op is neither open nor close
                _trace(2, surface="locks", op="open"),
                _trace(3, surface="locks", op="close"),  # balanced → no leak
            ),
        )

        assert inv(history) == []

    def test_detects_imbalance(self) -> None:
        inv = no_resource_leak(open_op="open", close_op="close", by="route")
        history = History(
            seed=0,
            events=(
                _trace(0, op="open", route="r1"),
                _trace(1, op="open", route="r1"),
                _trace(2, op="close", route="r1"),  # one open left dangling
            ),
        )

        violations = inv(history)

        assert len(violations) == 1
        assert violations[0].invariant == "no_resource_leak"


# ....................... #


class TestSingleKeyPerOperation:
    def test_no_matching_spans(self) -> None:
        inv = single_key_per_operation("pay")

        assert inv(History(seed=0, events=())) == []

    def test_step_outside_any_span(self) -> None:
        inv = single_key_per_operation("pay", surface="document_command")
        history = History(
            seed=0,
            events=(
                Event(
                    seq=0,
                    kind="operation",
                    at=0.0,
                    fields={"op": "pay", "start_seq": 10, "end_seq": 20},
                ),
                # trace_seq 5 falls outside [10, 20] → credited to no span
                _trace(1, surface="document_command", key="k1", trace_seq=5),
            ),
        )

        assert inv(history) == []


# ....................... #


class TestReadYourWrites:
    def test_skips_other_surface_and_unkeyed_events(self) -> None:
        inv = read_your_writes("document_command", value_field="amount")
        history = History(
            seed=0,
            events=(
                # different surface → skipped
                _trace(0, surface="other", key="k", payload={"amount": 1}),
                # right surface but no key → skipped
                _trace(1, surface="document_command", payload={"amount": 5}),
                # keyed write then a matching read → no violation
                _trace(2, surface="document_command", key="k", payload={"amount": 7}),
                _trace(3, surface="document_command", key="k", result={"amount": 7}),
            ),
        )

        assert inv(history) == []

    def test_detects_stale_read(self) -> None:
        inv = read_your_writes("document_command", value_field="amount")
        history = History(
            seed=0,
            events=(
                _trace(0, surface="document_command", key="k", payload={"amount": 7}),
                _trace(1, surface="document_command", key="k", result={"amount": 3}),
            ),
        )

        violations = inv(history)

        assert len(violations) == 1
        assert violations[0].invariant == "read_your_writes"
