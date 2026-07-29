"""The contract-misuse corpus — ground-truth defect instances, P1 slice.

Hand-authored broken twins of correct Forze workloads (one seeded contract misuse each) plus
known-correct negative controls, registered against the :mod:`forze_dst.misuse` schema. The
operator taxonomy and per-operator citations live in ``OPERATORS.md`` next to this module; the
per-build smoke tier (``tests/unit/test_forze_dst/test_misuse_corpus.py``) replays every mutant's
killing seed and every control's clean band.

Mining provenance (2026-07-29): killing entries and depth evidence produced by a sweep over
``seeds=range(400)`` per mutant under the recorded explore knobs; depth labels verified
empirically — every d=2 mutant is clean at ``concurrency=1`` over seeds 0..199 (an adverse
interleaving is required), every d=1 mutant kills there (the duplicated workload alone
suffices). Mechanical depth extraction (1-minimal choice vectors) lands with the P2 tooling.
"""

from __future__ import annotations

from forze_dst.artifacts.corpus import RegressionEntry
from forze_dst.misuse import (
    GroundTruth,
    MisuseControl,
    MisuseFamily,
    MisuseMutant,
    TransferTier,
)

# ----------------------- #

_FOUND_AT = "2026-07-29"
_SCHEDULE_SEED = 4223464447449377271  # derive_seed(0, "schedule") — recorded from the mining run

_CONCURRENT = {"strategy": "scenario", "act_count": 4, "concurrency": 3}
_SEQUENTIAL = {"strategy": "scenario", "act_count": 4, "concurrency": 1}

_T_CAMPAIGN = {"strategy": "scenario", "act_count": 2, "concurrency": 2, "pool": 16}
_SEQ_CAMPAIGN = {"strategy": "scenario", "act_count": 2, "concurrency": 1, "pool": 16}

SMOKE_CONTROL_EXPLORE = _CONCURRENT
"""The corpus-wide explore knobs every control's clean band runs under (mutants replay their own)."""

_T_MECHANICAL = (
    "mechanical (extract_depth): d = 1 + 0 non-FIFO choices — the 1-minimal schedule is EMPTY: "
    "plain FIFO at the recorded knobs (act_count=4, concurrency=3, workload seed 0) already "
    "violates. CORRECTS the earlier manual d=2 label, which conflated workload concurrency "
    "(two in-flight ops ARE required — clean at concurrency=1 over seeds 0..199) with scheduler "
    "ordering constraints: under the locked PCT-aligned definition the scheduler needs zero."
)
_D1_MECHANICAL = (
    "mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal schedule () "
    "(act_count=4, concurrency=1, workload seed 0) — the duplicated delivery alone suffices."
)

_FAULT_EVIDENCE = (
    "fault: d=1 by construction — the defect is triggered by the crash fault (the process dies "
    "between the two commits), not by an ordering constraint; it manifests under FIFO at "
    "concurrency=1 with the case's CrashPolicy. Systematic choice-vector extraction runs "
    "faultless by design, so it does not apply; the crash point is part of the seeded fault "
    "stream and reproduces from the run seed."
)

_PAY_FINGERPRINT = "sha256:7265dbffb577ac90c9a619bf2eb522d26a3067129409cf425ee30dc6f43cb0f9"


def _kill(target: str, *, fingerprint: str, invariants: tuple[str, ...], explore: dict) -> RegressionEntry:  # type: ignore[type-arg]
    return RegressionEntry(
        seed=0,
        schedule_seed=_SCHEDULE_SEED,
        target=target,
        registry_fingerprint=fingerprint,
        invariants=invariants,
        found_at=_FOUND_AT,
        explore=explore,
    )


# ....................... #

CORPUS: tuple[MisuseMutant, ...] = (
    MisuseMutant(
        mutant_id="T1-blind-write-payment",
        operator="T1 drop_rev_guard",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.transactions:t1_blind_write_payment",
        campaign_base="tests.support.misuse.transactions:t1_blind_write_payment_campaign",
        campaign_explore=_T_CAMPAIGN,
        summary="The pay transition uses a blind bulk write instead of the rev-guarded update; "
        "every concurrent payer wins and charges.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.transactions:t1_blind_write_payment",
            fingerprint=_PAY_FINGERPRINT,
            invariants=("expect",),
            explore=_CONCURRENT,
        ),
        depth=1,
        depth_evidence=_T_MECHANICAL,
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
    ),
    MisuseMutant(
        mutant_id="T2-charge-before-guard",
        operator="T2 effect_before_guard",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.transactions:t2_charge_before_guard",
        campaign_base="tests.support.misuse.transactions:t2_charge_before_guard_campaign",
        campaign_explore=_T_CAMPAIGN,
        summary="An external (non-transactional) charge fires before the rev-guarded transition; "
        "the loser's rollback cannot recall it.",
        expected_invariants=("no_duplicate_effect",),
        killing=_kill(
            "tests.support.misuse.transactions:t2_charge_before_guard",
            fingerprint=_PAY_FINGERPRINT,
            invariants=("no_duplicate_effect",),
            explore=_CONCURRENT,
        ),
        depth=1,
        depth_evidence=_T_MECHANICAL,
        port_observable=False,
        transfer_tier=TransferTier.NOT_TRANSFERABLE,
        notes="The external charge is a trace-level marker by design (an effect that leaves the "
        "process, not a port write) — no real-backend final-state observable exists. The "
        "transferable sibling of this shape is T3.",
    ),
    MisuseMutant(
        mutant_id="T3-payment-outside-tx",
        operator="T3 write_outside_tx",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.transactions:t3_payment_outside_tx",
        campaign_base="tests.support.misuse.transactions:t3_payment_outside_tx_campaign",
        campaign_explore=_T_CAMPAIGN,
        summary="The row-before-guard handler with its transaction boundary removed; the loser's "
        "charge row survives its failed transition.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.transactions:t3_payment_outside_tx",
            fingerprint=_PAY_FINGERPRINT,
            invariants=("expect",),
            explore=_CONCURRENT,
        ),
        depth=1,
        depth_evidence=_T_MECHANICAL,
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
    ),
    MisuseMutant(
        mutant_id="T3-torn-activation",
        operator="T3 write_outside_tx",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.activation:t3_torn_activation",
        summary="Create and activate a profile in separate transactions; a phase-padded reader "
        "can observe the torn created-but-not-ready window.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=1,
            schedule_seed=None,
            target="tests.support.misuse.activation:t3_torn_activation",
            registry_fingerprint="sha256:633fa9cc04c0ea63da16c1e8db5f6deb8096dfdc9d699910714debd135bf4eeb",
            invariants=("expect",),
            found_at=_FOUND_AT,
            explore={"strategy": "scenario", "act_count": 2, "concurrency": 2},
        ),
        depth=2,
        depth_evidence=(
            "mechanical (extract_depth): d = 1 + 1 non-FIFO choice in the 1-minimal schedule "
            "(0, 0, 0, 1) (act_count=2, concurrency=2, workload seed 1) — plain FIFO is clean in "
            "both spawn orders (SERVE_PADDING=2 phase alignment); padding 0-1 degenerates to d=1, "
            "padding >=3 makes the window unreachable within 40k systematic runs. The corpus's "
            "first genuinely depth-2 instance; p-hat ~= 0.26 under the random scheduler, so the "
            "base regime is naturally de-saturated (no campaign pool needed)."
        ),
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Second instance of the T3 operator: the same misuse (a write outside the "
        "transaction boundary), instantiated deep — the observable needs a specific overtake, "
        "not just concurrent overlap.",
    ),
    MisuseMutant(
        mutant_id="T5-unchecked-reservation",
        operator="T5 check_then_act",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.transactions:t5_unchecked_reservation",
        campaign_base="tests.support.misuse.transactions:t5_unchecked_reservation_campaign",
        campaign_explore=_T_CAMPAIGN,
        summary="Unguarded read-check-insert over the reservation aggregate (TOCTOU); two "
        "concurrent reservers both see zero and both insert.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.transactions:t5_unchecked_reservation",
            fingerprint="sha256:e36af77a168eae1f093ae2ca7aa7602723dadaa054f4f34c0fddc489a9a93642",
            invariants=("expect",),
            explore=_CONCURRENT,
        ),
        depth=1,
        depth_evidence=_T_MECHANICAL,
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
    ),
    MisuseMutant(
        mutant_id="I1-retry-without-key",
        operator="I1 drop_idempotency_key",
        family=MisuseFamily.IDEMPOTENCY,
        base="tests.support.misuse.idempotency:i1_retry_without_key",
        campaign_base="tests.support.misuse.idempotency:i1_retry_without_key_campaign",
        campaign_explore=_SEQ_CAMPAIGN,
        summary="A retried command appends a fresh charge row per delivery — no idempotency key "
        "to collapse the duplicates.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.idempotency:i1_retry_without_key",
            fingerprint="sha256:b1c6b29559d6cc66aa38ace1f9c26696896b12d8968ee4567dcd2a6eed5454ce",
            invariants=("expect",),
            explore=_SEQUENTIAL,
        ),
        depth=1,
        depth_evidence=_D1_MECHANICAL,
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Transfer is a plain re-invocation — no forced interleaving needed.",
    ),
    MisuseMutant(
        mutant_id="M1-dual-write-shipment",
        operator="M1 outbox_outside_tx",
        family=MisuseFamily.MESSAGING,
        base="tests.support.misuse.messaging:m1_dual_write_shipment",
        summary="State and its outbox event commit in separate transactions; a crash between "
        "the commits strands state whose event never leaves.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=0,
            schedule_seed=None,
            target="tests.support.misuse.messaging:m1_dual_write_shipment",
            registry_fingerprint="sha256:3bec22f2dc54ffeb7e902f8c35bd0fe39e07aa3499c2349c723ea17336b08bed",
            invariants=("expect",),
            found_at=_FOUND_AT,
            explore={
                "strategy": "scenario",
                "act_count": 3,
                "concurrency": 1,
                "crash_surface": "document_command",
                "crash_probability": 0.25,
            },
        ),
        depth=1,
        depth_evidence=_FAULT_EVIDENCE,
        port_observable=True,
        transfer_tier=TransferTier.FAULT_ANALOG,
        ground_truth=GroundTruth.REAL,
        notes="The canonical event-driven dual write. Runs under the crash-restart engine "
        "(the case carries its CrashPolicy); transfer analog: the crash is the session "
        "abandoning after the first commit.",
    ),
    MisuseMutant(
        mutant_id="I3-ack-before-processing",
        operator="I3 ack_before_processing",
        family=MisuseFamily.IDEMPOTENCY,
        base="tests.support.misuse.idempotency:i3_ack_before_processing",
        summary="The ack commits before the effect; a crash in the window loses the effect "
        "forever — the redelivery sees the ack and skips.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=0,
            schedule_seed=None,
            target="tests.support.misuse.idempotency:i3_ack_before_processing",
            registry_fingerprint="sha256:20cdd1da203432f33b41399333101e06df19d2bf5aab8cf06382dc0d937a01a6",
            invariants=("expect",),
            found_at=_FOUND_AT,
            explore={
                "strategy": "scenario",
                "act_count": 3,
                "concurrency": 1,
                "crash_surface": "document_command",
                "crash_probability": 0.25,
            },
        ),
        depth=1,
        depth_evidence=_FAULT_EVIDENCE,
        port_observable=True,
        transfer_tier=TransferTier.FAULT_ANALOG,
        ground_truth=GroundTruth.REAL,
        notes="At-most-once where at-least-once was required. Runs under the crash-restart "
        "engine; transfer analog: abandonment after the ack commit, then a redelivery.",
    ),
    MisuseMutant(
        mutant_id="M2-consumer-without-inbox",
        operator="M2 drop_inbox_dedup",
        family=MisuseFamily.MESSAGING,
        base="tests.support.misuse.messaging:m2_consumer_without_inbox",
        campaign_base="tests.support.misuse.messaging:m2_consumer_without_inbox_campaign",
        campaign_explore=_SEQ_CAMPAIGN,
        summary="A consumer applies its effect on every delivery — no inbox table, so a "
        "redelivered message is processed twice.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.messaging:m2_consumer_without_inbox",
            fingerprint="sha256:de5d34e0a6b5708a6439424bbbfae1bc9f76d565c50baf11c3e1b1714897398d",
            invariants=("expect",),
            explore=_SEQUENTIAL,
        ),
        depth=1,
        depth_evidence=_D1_MECHANICAL,
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Transfer is a plain re-delivery — no forced interleaving needed.",
    ),
)


# ....................... #

CONTROLS: tuple[MisuseControl, ...] = (
    MisuseControl(
        control_id="ctrl-row-after-guard",
        base="tests.support.misuse.transactions:ctrl_row_after_guard",
        summary="The plain correct payment: charge row only after the rev-guarded transition, "
        "all in one transaction.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-row-before-guard-in-tx",
        base="tests.support.misuse.transactions:ctrl_row_before_guard_in_tx",
        summary="Effect-before-guard SHAPED but correct: the charge row lands first, and the "
        "transaction rolls it back with the loser's failed transition.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-atomic-provision",
        base="tests.support.misuse.activation:ctrl_atomic_provision",
        summary="The atomic provision: create and activate in one transaction — no schedule at "
        "any depth can expose a torn state.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-unique-reservation",
        base="tests.support.misuse.transactions:ctrl_unique_reservation",
        summary="The reservation id derives from the user — the unique key closes the "
        "check-then-act race without any check.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-retry-with-key",
        base="tests.support.misuse.idempotency:ctrl_retry_with_key",
        summary="Retry SHAPED but correct: the same duplicated workload, deduplicated by the "
        "command-id-derived row key.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-outbox-in-tx",
        base="tests.support.misuse.messaging:ctrl_outbox_in_tx",
        summary="The outbox pattern: state and its event in one transaction — a crash leaves "
        "both or neither, never state without its event.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-process-then-ack",
        base="tests.support.misuse.idempotency:ctrl_process_then_ack",
        summary="Effect and ack in one transaction — a crash rolls both back and the "
        "redelivery completes the work.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-inbox-consumer",
        base="tests.support.misuse.messaging:ctrl_inbox_consumer",
        summary="The inbox pattern: message id inserted first, effect in the same transaction — "
        "redeliveries conflict and stop.",
        adversarial=False,
        clean_band=(0, 32),
    ),
)
