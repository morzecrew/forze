"""The assertion toolkit — everything you pass to ``Simulation(invariants=[...])``.

A namespace over the oracle's assertion vocabulary: always-properties (``no_duplicate_effect``,
``monotonic_per``, ``mutual_exclusion``, …), value-level checks (``expect``, ``expect_value``,
``read_your_writes``), transactional isolation (``snapshot_isolation``, ``serializable``),
linearizability (``linearizable`` + its specs), commutativity (``commutative_convergence`` — the one
*cross-history* checker, not a single-history :data:`Invariant`), and reachability / "sometimes"
liveness (``sometimes``, ``reached``). Import the namespace and reach for what you need:
``from forze_dst import invariants as inv`` → ``inv.no_duplicate_effect(...)``.
"""

from __future__ import annotations

from forze_dst.oracle.commutativity import commutative_convergence
from forze_dst.oracle.system_invariants import CompiledOracle, compile_oracle
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
from forze_dst.oracle.isolation import (
    TxRecord,
    VersionedTxRecord,
    find_serializability_cycle,
    find_serializable_violations,
    find_snapshot_isolation_violations,
    serializable,
    snapshot_isolation,
    transactions_from_history,
    versioned_transactions_from_history,
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
    # transactional isolation
    "snapshot_isolation",
    "serializable",
    "transactions_from_history",
    "versioned_transactions_from_history",
    "find_snapshot_isolation_violations",
    "find_serializable_violations",
    "find_serializability_cycle",
    "TxRecord",
    "VersionedTxRecord",
    # commutativity (cross-history)
    "commutative_convergence",
    # cross-aggregate system-invariant oracle
    "compile_oracle",
    "CompiledOracle",
    # reachability / liveness
    "sometimes",
    "reached_labels",
    "assess_reachability",
    "ReachabilityReport",
]
