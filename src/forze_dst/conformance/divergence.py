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

    engine: str | None = None
    """The backend scope name this strengthening is specific to (``None`` = every backend).

    A ``None`` entry records a *Forze* strengthening (e.g. rev-OCC) that every backend — mock and
    real — exhibits identically. An engine-scoped entry records a strengthening one engine provides
    by construction (e.g. Mongo transactions are snapshot-isolated even at ``READ_COMMITTED``); it
    must not leak into another backend's oracle, where the textbook verdict still stands.
    """


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
    *(
        ContractStrengthening(
            anomaly=anomaly,
            level=IsolationLevel.READ_COMMITTED,
            engine="mongo",
            contract=Verdict.PERMITTED,
            observed=Verdict.PREVENTED,
            reason=(
                "MongoDB multi-document transactions execute every read against a single "
                "WiredTiger snapshot taken at the first operation, regardless of the "
                "transaction's read concern — the read concern level changes which writes the "
                "snapshot may contain (local vs majority-committed), never per-statement "
                "freshness. READ_COMMITTED inside a transaction is therefore not a distinct "
                "weaker level on Mongo: the read-path anomalies textbook read committed permits "
                "(a re-read, a cross-item read, a re-scan observing a concurrent commit) and the "
                "stale-snapshot write (fresh_read_update: the re-read returns the as-of-begin row, "
                "so the write conflicts instead of committing on the fresh value) are all governed "
                "by the snapshot, exactly as at SNAPSHOT. Asserted, not assumed: the Mongo "
                "differential runs the battery at READ_COMMITTED against a real replica set."
            ),
            source=(
                "MongoDB manual, Read Concern & Transactions (WiredTiger snapshot semantics); "
                "asserted by the Mongo differential leg"
            ),
        )
        for anomaly in ("non_repeatable_read", "read_skew", "phantom", "fresh_read_update")
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
    MechanismDivergence(
        name="skip-locked-degrades-to-for-update",
        reason=(
            "Postgres `SELECT ... FOR UPDATE SKIP LOCKED` returns DISJOINT rows to concurrent "
            "workers (a locked row is skipped, not waited on). The mock does not model the "
            "disjoint claim: `skip_locked` degrades to plain `FOR UPDATE` conflict-on-read (both "
            "workers claim the same row, one aborts) — the declared RowLockMode fallback for "
            "non-Postgres backends, not a silent no-op. A workload that depends on disjoint-claim "
            "work distribution is a declared mock gap; model it against real Postgres."
        ),
        source="Postgres docs, SELECT FOR UPDATE SKIP LOCKED; RowLockMode contract",
    ),
    MechanismDivergence(
        name="lock-block-vs-abort-conductor",
        reason=(
            "A duplicate-key insert race and a `FOR UPDATE` lock contention both BLOCK the contender "
            "on Postgres (it waits for the holder to commit, then raises 23505 / re-reads the fresh "
            "row) — which would wedge a one-participant-at-a-time forced interleaving. The mock is "
            "abort-based, so it surfaces the same outcome (unique violation / no lost update) by "
            "conflicting at commit instead of blocking. The block is converted into the same explicit "
            "signal by the `_drive_lock_race` driver (arrive_blocking → commit the holder → release "
            "the contender), so both `abort_engine_only` cases run against real Postgres too; the "
            "generic parametrized legs (vanilla one-at-a-time Conductor) skip them, and a dedicated "
            "lock-race differential asserts the outcome against the real engine. Note the FOR UPDATE "
            "verdict is the final value (was an update lost?), not whether a transaction aborted: "
            "Postgres READ COMMITTED commits BOTH writers and loses nothing (the locked re-read sees "
            "the committed value), whereas the mock and Postgres SNAPSHOT/SERIALIZABLE abort the loser. "
            "MongoDB is abort-based like the mock but with no lock to wait on: the contender's "
            "conflicting write raises WriteConflict (code 112, mapped to a concurrency conflict) "
            "immediately — the duplicate-key insert conflicts on the peer's uncommitted index entry, "
            "and FOR UPDATE (degraded to a plain transactional read on Mongo, the declared RowLockMode "
            "fallback) prevents the lost update via the document-level write conflict instead of a row "
            "lock — so the same _drive_lock_race script runs against a real replica set and both cases "
            "land on the same PREVENTED verdict at every level Mongo advertises."
        ),
        source="battery docstring; _drive_lock_race; Postgres FOR UPDATE EvalPlanQual; ept/hermitage",
    ),
    MechanismDivergence(
        name="read-only-abort-vs-safe-snapshot",
        reason=(
            "On a phantom under serializable, the mock's coarse namespace-level read-set aborts even "
            "a read-only transaction that scanned a namespace a concurrent transaction then wrote, "
            "whereas an SSI engine (Postgres) commits it via the read-only safe-snapshot optimization "
            "(no dangerous cycle runs through a read-only pivot). Both PREVENT the phantom — the "
            "reader observes its frozen snapshot either way — so the differential compares the frozen "
            "scan result, never whether the read-only transaction aborted."
        ),
        source="Cahill/Fekete, Serializable Snapshot Isolation (read-only safe-retry optimization)",
    ),
    MechanismDivergence(
        name="outbox-inbox-write-through",
        reason=(
            "Only the DOCUMENT store gets MVCC isolation (writes buffered to a commit-time overlay); "
            "the outbox (`list`) and inbox (`set`) adapters journal write-through — a row is appended "
            "to live state immediately and reverted by a per-element undo thunk on rollback. Whole-store "
            "snapshot isolation for them would serialize concurrent transactions and blind DST to the "
            "interleavings it exists to explore, so this is deliberate. ATOMICITY still holds — a "
            "rolled-back transaction leaves no outbox/inbox rows, so it cannot produce a "
            "double-publish-from-abort finding — but a concurrent still-in-flight transaction CAN read "
            "another's not-yet-committed outbox/inbox rows (a dirty read Postgres READ COMMITTED would "
            "not permit). Treat a premature-visibility / phantom-event finding on the outbox→relay→inbox "
            "path as possible mock over-visibility and confirm it against a real broker/store. This is a "
            "CHECKED divergence: `observe_uncommitted_outbox_visibility` asserts the mock over-permits "
            "and real Postgres prevents it, from both ends. The crash-recovery delivery path is instead "
            "verdict-EQUIVALENT (atomicity holds — a rolled-back transaction leaves no rows), pinned by "
            "`run_crash_recovery_delivery` as mock ≡ real Postgres."
        ),
        source="forze_mock journal design (_journal.py, adapters/tx.py MockJournalTxManagerAdapter)",
    ),
)
"""Mock-vs-real surface differences the differential leg must normalize, not flag (forward-looking)."""
