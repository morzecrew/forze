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

Scope: the isolation family only. A general "mock ≡ real for every port" matrix is deliberately out —
it has no shared equivalence relation and would drown in false positives.
"""

from __future__ import annotations

from .anomalies import BATTERY, AnomalyCase, expected_verdict
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

# ----------------------- #

__all__ = [
    "AnomalyCase",
    "BATTERY",
    "expected_verdict",
    "ConformanceBackend",
    "Verdict",
    "is_serialization_conflict",
    "record_outcome",
    "ContractStrengthening",
    "MechanismDivergence",
    "CONTRACT_STRENGTHENINGS",
    "MECHANISM_DIVERGENCES",
]
