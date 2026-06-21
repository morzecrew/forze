"""The assertion toolkit — everything you pass to ``Simulation(invariants=[...])``.

A namespace over the oracle's assertion vocabulary: always-properties (``no_duplicate_effect``,
``monotonic_per``, ``mutual_exclusion``, …), value-level checks (``expect``, ``expect_value``,
``read_your_writes``), linearizability (``linearizable`` + its specs), and reachability /
"sometimes" liveness (``sometimes``, ``reached``). Import the namespace and reach for what you
need: ``from forze_dst import invariants as inv`` → ``inv.no_duplicate_effect(...)``.
"""

from __future__ import annotations

from forze_dst.oracle.invariants import (
    Invariant,
    Violation,
    check,
    completes_within,
    expect,
    expect_value,
    monotonic_per,
    mutual_exclusion,
    no_duplicate_effect,
    no_resource_leak,
    no_unclosed_transaction,
    no_unexpected_error,
    operation_succeeds,
    read_your_writes,
    single_key_per_operation,
)
from forze_dst.oracle.linearizability import (
    RegisterSpec,
    SequentialSpec,
    is_linearizable,
    linearizable,
    monotonic_reads,
    record_operation,
    sequential,
)
from forze_dst.oracle.reachability import (
    ReachabilityReport,
    assess_reachability,
    reached_labels,
    sometimes,
)

# ....................... #

__all__ = [
    # types
    "Invariant",
    "Violation",
    "check",
    # always-properties
    "no_duplicate_effect",
    "no_resource_leak",
    "no_unclosed_transaction",
    "monotonic_per",
    "mutual_exclusion",
    "no_unexpected_error",
    "operation_succeeds",
    "completes_within",
    "single_key_per_operation",
    # value-level
    "expect",
    "expect_value",
    "read_your_writes",
    # linearizability + weaker consistency models
    "linearizable",
    "is_linearizable",
    "sequential",
    "monotonic_reads",
    "RegisterSpec",
    "SequentialSpec",
    "record_operation",
    # reachability / liveness
    "sometimes",
    "reached_labels",
    "assess_reachability",
    "ReachabilityReport",
]
