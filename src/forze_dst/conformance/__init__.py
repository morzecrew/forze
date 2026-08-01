"""Adapter conformance — prove the mock behaves like the real adapter at the declared isolation level.

DST's oracle horizon is the mock port: every invariant it proves holds against ``forze_mock``, not
against Postgres/Redis/Mongo. This package closes that gap for the transactional-isolation family. It
ships a battery of classic isolation anomalies (Adya phenomena) as deterministic forced
interleavings with a known verdict per :class:`~forze.application.contracts.transaction.IsolationLevel`,
runnable against any :class:`ConformanceBackend` — the in-memory mock or, via the differential leg, a
real backend over testcontainers — so "passed on the mock" can finally mean "matches the real engine".

The battery is **adapter-agnostic**: bring a backend (N independent sessions over one shared store)
and run it. The verdict is normalized to ``PERMITTED``/``PREVENTED`` so the differential compares the
anomaly OUTCOME at the declared level, never the mechanism, error code, or victim. The
:data:`CONTRACT_STRENGTHENINGS` / :data:`MECHANISM_DIVERGENCES` catalogs record, as reviewed data, the
differences that are expected and must not be flagged — the firewall that keeps the differential from
dying of false positives.

Scope: three families where the mock is a stand-in for a real engine and the equivalence relation is
sharp — the **transactional-isolation** anomalies (verdict per level), the **outbox→inbox delivery
semantics under a crash** (:func:`run_crash_recovery_delivery`: at-least-once + exactly-once effect
across the publish-then-crash window), and the **realtime gateway's ack-stream delivery under a
crash** (:func:`run_gateway_crash_delivery`: exactly-once emit + store-then-forward atomicity across
the bridge-then-crash windows, with the real gateway bridge injected by the test). A general
"mock ≡ real for every port" matrix stays out — it has no shared equivalence relation and would
drown in false positives.
"""

from __future__ import annotations

from .anomalies import BATTERY, AnomalyCase, expected_verdict
from .catalog import (
    PLANE_DIVERGENCES,
    DivergenceResolution,
    EngineBehaviour,
    PlaneDivergence,
)
from .counters import (
    EXPECTED_ALLOCATION,
    EXPECTED_CEILING_PORTABLE,
    EXPECTED_PARTITIONS,
    CeilingOutcome,
    CounterOutcome,
    PartitionOutcome,
    run_counter_allocation,
    run_counter_ceiling,
    run_counter_partitions,
)
from .delivery import (
    DELIVERY_EVENTS,
    DELIVERY_INBOX,
    DELIVERY_OUTBOX,
    DeliveryOutcome,
    DeliveryPayload,
    observe_uncommitted_outbox_visibility,
    run_crash_recovery_delivery,
)
from .divergence import (
    CONTRACT_STRENGTHENINGS,
    MECHANISM_DIVERGENCES,
    ContractStrengthening,
    MechanismDivergence,
)
from .harness import (
    ConformanceBackend,
    Verdict,
    is_serialization_conflict,
    record_outcome,
)
from .inference import (
    CapabilityGateOutcome,
    GateVerdict,
    run_capability_gates,
)
from .realtime import (
    REALTIME_DELIVERY_GROUP,
    REALTIME_DELIVERY_PRINCIPAL,
    REALTIME_DELIVERY_SIGNALS,
    GatewayCrashPoint,
    GatewayDeliveryOutcome,
    RealtimeBridge,
    run_gateway_crash_delivery,
)
from .report import (
    CellVerdict,
    Classification,
    FidelityMatrix,
    MatrixCell,
    collect_verdicts,
    render_markdown,
    write_matrix,
)
from .transfer import (
    Detection,
    TransferClassification,
    TransferRecord,
    TransferScript,
    divergences,
    render_transfer_markdown,
    run_transfer,
    write_transfer,
)

# ----------------------- #

__all__ = [
    "AnomalyCase",
    "BATTERY",
    "expected_verdict",
    "PLANE_DIVERGENCES",
    "DivergenceResolution",
    "EngineBehaviour",
    "PlaneDivergence",
    "CeilingOutcome",
    "CounterOutcome",
    "PartitionOutcome",
    "EXPECTED_ALLOCATION",
    "EXPECTED_CEILING_PORTABLE",
    "EXPECTED_PARTITIONS",
    "run_counter_allocation",
    "run_counter_ceiling",
    "run_counter_partitions",
    "CapabilityGateOutcome",
    "GateVerdict",
    "run_capability_gates",
    "CellVerdict",
    "Classification",
    "FidelityMatrix",
    "MatrixCell",
    "collect_verdicts",
    "render_markdown",
    "write_matrix",
    "Detection",
    "TransferClassification",
    "TransferRecord",
    "TransferScript",
    "divergences",
    "render_transfer_markdown",
    "run_transfer",
    "write_transfer",
    "ConformanceBackend",
    "Verdict",
    "is_serialization_conflict",
    "record_outcome",
    "ContractStrengthening",
    "MechanismDivergence",
    "CONTRACT_STRENGTHENINGS",
    "MECHANISM_DIVERGENCES",
    "run_crash_recovery_delivery",
    "observe_uncommitted_outbox_visibility",
    "run_gateway_crash_delivery",
    "GatewayCrashPoint",
    "GatewayDeliveryOutcome",
    "RealtimeBridge",
    "REALTIME_DELIVERY_GROUP",
    "REALTIME_DELIVERY_PRINCIPAL",
    "REALTIME_DELIVERY_SIGNALS",
    "DeliveryOutcome",
    "DeliveryPayload",
    "DELIVERY_OUTBOX",
    "DELIVERY_INBOX",
    "DELIVERY_EVENTS",
]
