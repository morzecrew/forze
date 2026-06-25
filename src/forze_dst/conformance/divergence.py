"""The allowed-divergence catalog — the artifact that keeps the differential honest.

A mock-vs-real (or engine-vs-engine) differential dies if it flags every difference as a bug:
abort-vs-block, error-code text, victim identity, and "REPEATABLE READ means two different
things" are *expected* and must be ignored. This module is that firewall, as reviewed data — not
prose — split into two kinds:

- :data:`CONTRACT_STRENGTHENINGS` — a Forze adapter *prevents* an anomaly the textbook contract
  *permits*, by construction. These are checked: the battery asserts the observed verdict equals
  the contract overlaid with exactly these strengthenings, so a strengthening is the *only* way a
  verdict may deviate from the textbook (no silent divergence).
- :data:`MECHANISM_DIVERGENCES` — surface differences between two correct engines that the
  real-adapter differential leg must normalize away (it compares the anomaly outcome, never the
  mechanism). Documented now so the differential is built against them from day one.

Each entry cites the source that justifies it; adding a backend means reviewing and extending the
catalog, and that review is the work.
"""

from __future__ import annotations

import attrs

from forze.application.contracts.transaction import IsolationLevel

from .harness import Verdict

# ----------------------- #


@attrs.frozen(kw_only=True)
class ContractStrengthening:
    """An adapter prevents an anomaly the textbook contract permits — a justified strengthening."""

    anomaly: str
    """The :class:`~forze_dst.conformance.AnomalyCase` name this applies to."""

    level: IsolationLevel
    """The level at which the strengthening applies."""

    contract: Verdict
    """What the Adya/Berenson contract permits at this level."""

    observed: Verdict
    """What a correct Forze adapter actually does (stronger than the contract)."""

    reason: str
    """Why the strengthening is correct and expected."""

    source: str
    """The reference that defines the textbook contract being strengthened."""


# ....................... #


@attrs.frozen(kw_only=True)
class MechanismDivergence:
    """A surface difference between correct engines the differential must ignore (not assert on)."""

    name: str
    reason: str
    source: str


# ....................... #


CONTRACT_STRENGTHENINGS: tuple[ContractStrengthening, ...] = (
    ContractStrengthening(
        anomaly="lost_update",
        level=IsolationLevel.READ_COMMITTED,
        contract=Verdict.PERMITTED,
        observed=Verdict.PREVENTED,
        reason=(
            "Forze's document update is compare-and-swap on the row `rev`: update(pk, rev, dto) "
            "rejects a stale rev. The textbook lost update assumes a blind read-modify-write, "
            "which the API does not offer, so lost update cannot occur at any level — a "
            "legitimate strengthening, not an isolation guarantee. Identical mock-vs-real: `rev` "
            "is an application column, so a real adapter's `UPDATE ... WHERE rev = :rev` affecting "
            "zero rows raises the same revision conflict."
        ),
        source="Berenson et al., A Critique of ANSI SQL Isolation Levels (P4, lost update)",
    ),
)
"""Anomalies a correct Forze adapter prevents that the textbook contract permits (checked)."""


# ....................... #


MECHANISM_DIVERGENCES: tuple[MechanismDivergence, ...] = (
    MechanismDivergence(
        name="abort-vs-block",
        reason=(
            "Snapshot/serializable engines abort the loser; lock-based engines block then proceed "
            "(or deadlock-abort). Both PREVENT the anomaly — normalize to PREVENTED before comparing."
        ),
        source="Cahill/Fekete, Serializable Snapshot Isolation; ept/hermitage",
    ),
    MechanismDivergence(
        name="error-code-or-sqlstate",
        reason=(
            "Postgres 40001, MySQL 1213/1205, Oracle ORA-08177, and the mock's serialization "
            "failure / revision conflict all mean 'retryable serialization failure'. Match the "
            "class, never the literal code or message."
        ),
        source="vendor documentation",
    ),
    MechanismDivergence(
        name="victim-identity",
        reason=(
            "First-committer-wins, first-updater-wins, and deadlock-victim heuristics differ on "
            "WHICH transaction aborts. The anomaly is prevented either way; victim identity is "
            "non-deterministic and must not be asserted."
        ),
        source="ept/hermitage",
    ),
    MechanismDivergence(
        name="repeatable-read-class",
        reason=(
            "REPEATABLE READ is true snapshot isolation on some engines (Postgres: abort-based, "
            "no phantoms, prevents lost update) and next-key locking on others (MySQL InnoDB: "
            "block-based, permits some write-predicate anomalies) — different guarantees under one "
            "SQL name. Parameterize the expected matrix by the engine's declared class."
        ),
        source="Kleppmann, Hermitage: Testing the 'I' in ACID",
    ),
)
"""Mock-vs-real surface differences the differential leg must normalize, not flag (forward-looking)."""
