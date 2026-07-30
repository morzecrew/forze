"""The contract-misuse corpus schema — ground-truth defect instances as reviewed data.

A *misuse mutant* is a hand-authored broken twin of a correct Forze workload: the same handlers
with exactly one seeded contract misuse (a dropped rev guard, an outbox write outside the
transaction, a retry without an idempotency key, …). Ground truth is by construction — each
mutant is known to contain exactly one defect, each *control* is known to contain none — which is
what makes the corpus usable as a measurement denominator: strategy comparisons, mock-fidelity
transfer runs, and seed statistics all need to know whether a verdict was right.

This module is only the schema. Instances live in the test tree (``tests/support/misuse/`` in
this repo; app authors can build their own corpora against their own workloads with the same
types). Every mutant carries a replayable :class:`~forze_dst.artifacts.corpus.RegressionEntry`
(the killing seed + the exploration knobs that found it), so a corpus doubles as a permanent
regression suite over the harness's ability to catch each misuse class.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, final

import attrs

from forze_dst.artifacts.corpus import RegressionEntry

if TYPE_CHECKING:
    from forze_dst.faults import CrashPolicy
    from forze_dst.harness import Simulation
    from forze_dst.scenario import Scenario

# ----------------------- #


class MisuseFamily(Enum):
    """The five defect families the operator taxonomy spans."""

    TRANSACTIONS = "transactions"
    """Concurrency & transactions: rev guards, boundaries, isolation, check-then-act."""

    IDEMPOTENCY = "idempotency"
    """Idempotency & retries: keys, naive retries, ack ordering."""

    MESSAGING = "messaging"
    """Messaging: outbox/inbox discipline, saga compensation."""

    DISTRIBUTED = "distributed"
    """Distributed primitives: locks, HLC, clocks."""

    DATA = "data"
    """Data & multitenancy: tenant predicates, cache invalidation, cursor binding."""


# ....................... #


class TransferTier(Enum):
    """How (whether) a mutant's trigger can run against a real backend (the mock↔real transfer)."""

    CONDUCTOR = "conductor"
    """The killing situation is a port-level ordering expressible as a forced interleaving (or a
    plain re-invocation), replayable identically on a real engine."""

    FAULT_ANALOG = "fault_analog"
    """Manifestation needs a fault (crash, redelivery); transfer uses the documented real-world
    analog (kill the connection, redeliver via the real broker)."""

    NOT_TRANSFERABLE = "not_transferable"
    """Needs simulation-only machinery (virtual time, trace-level observables); the reason is
    stated in :attr:`MisuseMutant.notes` and the fraction is reported, never silently dropped."""


# ....................... #


class GroundTruth(Enum):
    """Whether the seeded defect manifests on a real engine — set only by a real-backend run."""

    REAL = "real"
    MOCK_ARTIFACT = "mock_artifact"
    """The mock kills it but a real engine does not — everything DST "catches" via it is noise."""
    UNDETERMINED = "undetermined"


# ....................... #


@final
@attrs.frozen(kw_only=True)
class MisuseCase:
    """A runnable corpus case: the simulation plus the explicit workload that provokes it.

    Corpus workloads are explicit (a duplicate-delivery pool, a contested order id) — the
    auto-derived scenario generates well-behaved random inputs and would rarely provoke the
    seeded defect. ``scenario=None`` opts into auto-derivation where it does suffice.
    """

    simulation: Simulation
    scenario: Scenario | None = None

    crash: CrashPolicy | None = None
    """Set for crash-fault instances: the run becomes the crash → restart → recovery scenario
    (the defect is lost after-commit work / a partial non-transactional write). Runners thread
    this into ``SimulationConfig.crash`` for the mutant AND its controls — a control is only a
    control if it stays clean under the same crashes."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class MisuseMutant:
    """One corpus instance: a known-bug twin, its oracle, and its replayable killing entry."""

    mutant_id: str
    """Stable id, ``<operator>-<base>`` style (``"T1-blind-write-payment"``)."""

    operator: str
    """The misuse operator (``"T1 drop_rev_guard"`` — see the corpus ``OPERATORS.md`` table)."""

    family: MisuseFamily

    base: str
    """Import target ``module:attr`` of a **zero-argument factory** returning a fresh
    :class:`MisuseCase` (fresh closures/state per call, so runs never bleed)."""

    summary: str
    """What the seeded misuse is, in one sentence."""

    expected_invariants: tuple[str, ...]
    """Names of the invariant(s) expected to catch it — the smoke tier asserts one of these
    actually fired, not merely that *something* failed."""

    killing: RegressionEntry
    """The replayable kill: seed, exploration knobs, and the registry fingerprint at find time
    (a fingerprint drift means the replay can no longer be trusted — the smoke tier fails loud)."""

    depth: int
    """The bug depth ``d``, PCT-parameter-aligned: 1 + the number of non-FIFO scheduling choices
    in the 1-minimal reproducing schedule. The axis the seed-statistics experiments plot over."""

    depth_evidence: str
    """How ``d`` was derived — the minimized choice vector once the mechanical extraction tooling
    lands, until then an explicit manual derivation note (never a bare number)."""

    port_observable: bool
    """Whether the defect's manifestation is observable through port reads alone (``False`` =
    the oracle needs trace-level markers, which also caps transferability)."""

    transfer_tier: TransferTier

    ground_truth: GroundTruth = GroundTruth.UNDETERMINED
    """Starts undetermined; only the real-backend transfer run may set it."""

    campaign_base: str | None = None
    """Optional second factory (``module:attr`` → :class:`MisuseCase`): the **campaign** workload
    regime. The kill-fast smoke workload saturates (nearly every seed detects — no discriminating
    power between strategies), so detection-time campaigns run a de-saturated collision-pool
    variant where the race only fires when concurrent operations draw the same entity from a pool
    of size ``P`` (per-seed detection probability ≈ ``1/P``, tunable). ``None`` = campaigns fall
    back to :attr:`base`."""

    campaign_explore: dict[str, object] | None = None
    """The campaign regime's exploration knobs, recorded like ``killing.explore``."""

    notes: str = ""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.depth < 1:
            raise ValueError(f"{self.mutant_id}: depth must be >= 1, got {self.depth}")

        if not self.expected_invariants:
            raise ValueError(f"{self.mutant_id}: at least one expected invariant is required")

        if self.transfer_tier is TransferTier.NOT_TRANSFERABLE and not self.notes:
            # The declared fraction travels with its reasons — a bare NOT_TRANSFERABLE would
            # render as an unexplained exclusion.
            raise ValueError(
                f"{self.mutant_id}: NOT_TRANSFERABLE requires notes stating the reason"
            )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class MisuseControl:
    """A known-correct instance — the negative control that measures false positives.

    Adversarial controls are the load-bearing half: code *shaped like* a misuse that is
    nevertheless correct (a retry **with** its idempotency key; an effect-before-guard fully
    covered by transactional atomicity). A harness that flags them has a false-positive bug.
    """

    control_id: str

    base: str
    """Import target ``module:attr`` of a zero-argument factory returning a fresh :class:`MisuseCase`."""

    summary: str

    adversarial: bool
    """``True`` = deliberately misuse-shaped but correct; ``False`` = the plain correct twin."""

    clean_band: tuple[int, int]
    """The ``[start, stop)`` seed band asserted clean on every build."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        start, stop = self.clean_band
        if stop <= start:
            raise ValueError(f"{self.control_id}: clean_band must be a non-empty [start, stop)")
