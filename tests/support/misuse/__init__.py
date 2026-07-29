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

_DETERMINISTIC_NOTE = (
    "Deterministic manifestation: detection is not seed-dependent (the defect fires on "
    "essentially every workload that exercises it), so campaign statistics legitimately "
    "degenerate to p ~= 1 — this instance contributes corpus breadth and transfer ground "
    "truth, not detection-time discrimination."
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
        mutant_id="T3-double-torn",
        operator="T3 write_outside_tx",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.activation:t3_double_torn",
        summary="Provision creates and activates TWO profile halves in four separate "
        "transactions; the serve degrades gracefully on one torn half but blacks out when its "
        "two reads land in BOTH torn windows — which needs the writer stalled twice.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=1, schedule_seed=None,
            target="tests.support.misuse.activation:t3_double_torn",
            registry_fingerprint="sha256:633fa9cc04c0ea63da16c1e8db5f6deb8096dfdc9d699910714debd135bf4eeb",
            invariants=("expect",), found_at="2026-07-29",
            explore={"strategy": "scenario", "act_count": 2, "concurrency": 2},
        ),
        depth=3,
        depth_evidence="mechanical (extract_depth): d = 1 + 2 non-FIFO choices in the 1-minimal "
        "schedule (1, 0, 0, 0, 1) (act_count=2, concurrency=2, workload seed 1) — and stronger "
        "than 1-minimality: every single-nonzero-choice vector over the tick space was "
        "exhaustively refuted, so no depth-2 schedule kills. CAVEAT (measured): the mechanical "
        "(tick-promotion) and PCT (priority-stall) depth models diverge here — this bug needs "
        "four PCT priority segments, so pct-d3 does NOT recover the detection rate its "
        "parameter suggests (random ≈ 0.12 > pct-d4 ≈ 0.03 > pct-d3 ≈ 0.007 per seed); the "
        "PCT-bound comparison stays valid because the d=3 floor is far below all of them.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="The d-axis anchor above d=2: two separated stalls of one writer, phase-aligned "
        "so FIFO is clean and one promotion reaches at most one window (PAIR_SERVE_PADDING=2, "
        "adjacent reads).",
    ),
    MisuseMutant(
        mutant_id="T4-weakened-oncall",
        operator="T4 weaken_isolation",
        family=MisuseFamily.TRANSACTIONS,
        base="tests.support.misuse.transactions:t4_weakened_oncall",
        campaign_base="tests.support.misuse.transactions:t4_weakened_oncall_campaign",
        campaign_explore={"strategy": "scenario", "act_count": 2, "concurrency": 2, "pool": 16},
        summary="The on-call rota's read-both/write-own constraint declared at SNAPSHOT instead "
        "of SERIALIZABLE — write skew takes both doctors off call at once.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.transactions:t4_weakened_oncall",
            fingerprint="sha256:ef08758d8ca05fd5ae459fbd248cc6c63d6e361489a845d9f836b444a421f356",
            invariants=("expect",),
            explore=_CONCURRENT,
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal "
        "schedule () (act_count=2, concurrency=2, workload seed 2) — overlap alone suffices: "
        "FIFO lockstep already lands both reads before either commit, the write-skew shape.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="The seeded misuse is the declared IsolationLevel alone — the handler is byte-for-"
        "byte the control's. The campaign trigger needs a same-rota AND distinct-doctor "
        "concurrent pair, so p_trigger ≈ 1/(2·pool), not 1/pool.",
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
        mutant_id="I2-naive-retry-loop",
        operator="I2 retry_without_idempotency",
        family=MisuseFamily.IDEMPOTENCY,
        base="tests.support.misuse.idempotency:i2_retry_without_idempotency",
        campaign_base="tests.support.misuse.idempotency:i2_retry_without_idempotency_campaign",
        campaign_explore=_SEQ_CAMPAIGN,
        summary="A naive in-handler retry loop around a non-idempotent effect: the receipt "
        "commits in its own transaction, the per-order ack conflicts for the duplicate "
        "submission, and the re-run mints a second receipt for the same command.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.idempotency:i2_retry_without_idempotency",
            fingerprint="sha256:5879f7faf8612fbfde07510800369b1fcccad4ab8cb40403f4857b3ce8c6c9ee",
            invariants=("expect",),
            explore=_SEQUENTIAL,
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal "
        "schedule () (act_count=4, concurrency=1, workload seed 0) — the duplicate submission "
        "alone suffices; the loser's ack conflict fires sequentially too.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Distinct from I1: the retry is self-inflicted (an in-handler loop), and the "
        "effect escapes because it commits before the ack that detects the duplicate.",
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
    MisuseMutant(
        mutant_id="D1-skip-lock",
        operator="D1 skip_lock",
        family=MisuseFamily.DISTRIBUTED,
        base="tests.support.misuse.dlock:d1_skip_lock",
        summary="The critical section (read-modify-blind-write over a balance) runs without "
        "acquiring the lease — concurrent transfers lose updates.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=0, schedule_seed=None,
            target="tests.support.misuse.dlock:d1_skip_lock",
            registry_fingerprint="sha256:4fd3491e5da74cf76363e0b3a3ee610f61f0a574a293d93ca5b790df4f606f44",
            invariants=("expect",), found_at=_FOUND_AT,
            explore={"strategy": "scenario", "act_count": 3, "concurrency": 2},
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal schedule () "
        "(act_count=3, concurrency=2, workload seed 0).",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
    ),
    MisuseMutant(
        mutant_id="D2-early-lease-release",
        operator="D2 early_lock_release",
        family=MisuseFamily.DISTRIBUTED,
        base="tests.support.misuse.dlock:d2_early_lock_release",
        summary="The lease is released inside the critical section — after the read, before the "
        "write — so a spinning waiter acquires and reads the stale balance while the ex-holder's "
        "write is still in flight.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=2, schedule_seed=None,
            target="tests.support.misuse.dlock:d2_early_lock_release",
            registry_fingerprint="sha256:4fd3491e5da74cf76363e0b3a3ee610f61f0a574a293d93ca5b790df4f606f44",
            invariants=("expect",), found_at=_FOUND_AT,
            explore=_CONCURRENT,
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal "
        "schedule () (act_count=2, concurrency=2, workload seed 0) — FIFO lockstep itself walks "
        "the waiter's spin into the release→write hole before the ex-holder's write lands.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Naturally de-saturated without a pool: detection is a window lottery (the waiter "
        "must acquire inside the release→write hole), p̂ ≈ 0.5–0.8 under random schedules.",
    ),
    MisuseMutant(
        mutant_id="D3-nonatomic-acquire",
        operator="D3 nonatomic_acquire",
        family=MisuseFamily.DISTRIBUTED,
        base="tests.support.misuse.dlock:d3_nonatomic_acquire",
        summary="The lease is acquired check-then-set (count, then create a fresh-id row) — two "
        "acquirers both see it free and both enter the critical section.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=0, schedule_seed=None,
            target="tests.support.misuse.dlock:d3_nonatomic_acquire",
            registry_fingerprint="sha256:4fd3491e5da74cf76363e0b3a3ee610f61f0a574a293d93ca5b790df4f606f44",
            invariants=("expect",), found_at=_FOUND_AT,
            explore={"strategy": "scenario", "act_count": 3, "concurrency": 2},
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal schedule () "
        "(act_count=3, concurrency=2, workload seed 0).",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="The lock table is a lease row (DB-backed lock) — the atomic acquire is a "
        "unique-id create; the mutant re-derives T5's check-then-act at the lock layer.",
    ),
    MisuseMutant(
        mutant_id="D4-unmerged-remote-hlc",
        operator="D4 ignore_remote_hlc",
        family=MisuseFamily.DISTRIBUTED,
        base="tests.support.misuse.clock:d4_unmerged_remote_hlc",
        summary="The relay stamps its derived event from the local wall reading without merging "
        "the received timestamp — with the producer's clock ahead, the causal successor sorts "
        "below its cause.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=1, schedule_seed=None,
            target="tests.support.misuse.clock:d4_unmerged_remote_hlc",
            registry_fingerprint="sha256:5c8d525c99675b4ec951784dc22a2f41213187a493a4adb28b143fb8bd3adf40",
            invariants=("expect",), found_at="2026-07-29",
            explore=_SEQUENTIAL,
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal "
        "schedule () (act_count=4, concurrency=1, workload seed 1) — an emit followed by a relay "
        "suffices; no interleaving is involved.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Clock skew is workload data (the fast node stamps wall + 1h), not a time-source "
        "hack, so the identical provocation runs on a real backend. " + _DETERMINISTIC_NOTE,
    ),
    MisuseMutant(
        mutant_id="D5-wall-clock-ordering",
        operator="D5 nonmonotonic_clock",
        family=MisuseFamily.DISTRIBUTED,
        base="tests.support.misuse.clock:d5_wall_clock_ordering",
        summary="An ordering-critical stream stamped from raw wall readings — a fast-node "
        "append followed by a true-clock append runs the stream backwards.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.clock:d5_wall_clock_ordering",
            fingerprint="sha256:8f98dda1c9af85493481f81dbe744fe1c93b2b2882b2d637f6ab95e380475412",
            invariants=("expect",),
            explore=_SEQUENTIAL,
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal "
        "schedule () (act_count=4, concurrency=1, workload seed 0) — a fast append before a "
        "slow one suffices; no interleaving is involved.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes="Both twins guard the append with a seq-derived unique id — without it the "
        "CONTROL's read-max-write raced under concurrency, which its clean band caught during "
        "authoring; the seeded difference is the stamp discipline alone. " + _DETERMINISTIC_NOTE,
    ),
    MisuseMutant(
        mutant_id="N1-drop-tenant-predicate",
        operator="N1 drop_tenant_predicate",
        family=MisuseFamily.DATA,
        base="tests.support.misuse.tenancy:n1_drop_tenant_predicate",
        summary="The browse query drops the tenant filter — a viewer counts (sees) another "
        "tenant's rows: the cross-tenant leak.",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=1, schedule_seed=None,
            target="tests.support.misuse.tenancy:n1_drop_tenant_predicate",
            registry_fingerprint="sha256:43ba065ca51fa222f5c0328d512271e4506bd50bb4b26c5f45118f78acc6b5dc",
            invariants=("expect",), found_at=_FOUND_AT,
            explore={"strategy": "scenario", "act_count": 3, "concurrency": 1},
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal schedule () "
        "(act_count=3, concurrency=1, workload seed 1).",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes=_DETERMINISTIC_NOTE,
    ),
    MisuseMutant(
        mutant_id="N3-unbound-cursor-walk",
        operator="N3 cursor_unbound_tenant",
        family=MisuseFamily.DATA,
        base="tests.support.misuse.tenancy:n3_unbound_cursor_walk",
        summary="Page 1 filters by the viewer's tenant; the continuation trusts the cursor as a "
        "self-contained handle and drops the predicate — the keyset resume walks the other "
        "tenant's interleaved rows.",
        expected_invariants=("expect",),
        killing=_kill(
            "tests.support.misuse.tenancy:n3_unbound_cursor_walk",
            fingerprint="sha256:74076d3af6ae6842bff5b5525b4cde21de2162b629be5b4f2744f5d871633628",
            invariants=("expect",),
            explore={"strategy": "scenario", "act_count": 2, "concurrency": 1},
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal "
        "schedule () (act_count=2, concurrency=1, workload seed 0) — a single paged walk "
        "suffices; no interleaving is involved.",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes=_DETERMINISTIC_NOTE,
    ),
    MisuseMutant(
        mutant_id="N2-stale-cache",
        operator="N2 stale_cache",
        family=MisuseFamily.DATA,
        base="tests.support.misuse.tenancy:n2_stale_cache",
        summary="The write path never invalidates the read-through cache — even the writer's "
        "own read-through sees the stale version (read-your-writes broken).",
        expected_invariants=("expect",),
        killing=RegressionEntry(
            seed=0, schedule_seed=None,
            target="tests.support.misuse.tenancy:n2_stale_cache",
            registry_fingerprint="sha256:f130e35d8c549d3f7de588ecb16f9d59d9f6879ce546c5c1840e71ebeb07bbf1",
            invariants=("expect",), found_at=_FOUND_AT,
            explore={"strategy": "scenario", "act_count": 2, "concurrency": 1},
        ),
        depth=1,
        depth_evidence="mechanical (extract_depth): d = 1 + 0 non-FIFO choices in the 1-minimal schedule () "
        "(act_count=2, concurrency=1, workload seed 0).",
        port_observable=True,
        transfer_tier=TransferTier.CONDUCTOR,
        ground_truth=GroundTruth.REAL,
        notes=_DETERMINISTIC_NOTE,
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
        control_id="ctrl-atomic-pair",
        base="tests.support.misuse.activation:ctrl_atomic_pair",
        summary="Each half's create+activate commits atomically — no torn window exists, so the "
        "double-read serve can observe at worst an absent half (graceful partial).",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-idempotent-retry",
        base="tests.support.misuse.idempotency:ctrl_idempotent_retry",
        summary="The same naive retry loop as the I2 mutant, but the receipt id derives from "
        "the command — a re-run re-creates the same row and collapses into already-done.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-serializable-oncall",
        base="tests.support.misuse.transactions:ctrl_serializable_oncall",
        summary="The identical on-call handler declared at SERIALIZABLE — the serialization "
        "abort is caught and the doctor stays on call.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-merged-relay",
        base="tests.support.misuse.clock:ctrl_merged_relay",
        summary="The identical relay with the HLC merge rule — the derived stamp is lifted "
        "above the received timestamp, so causality survives any skew.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-floored-append",
        base="tests.support.misuse.clock:ctrl_floored_append",
        summary="The identical skewed-node append lifted above the stream's persisted "
        "high-water mark — monotone under any node mix.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-bound-cursor-walk",
        base="tests.support.misuse.tenancy:ctrl_bound_cursor_walk",
        summary="The same paged walk re-applying the tenant predicate with the same cursor — "
        "the resume stays inside the viewer's rows.",
        adversarial=True,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-release-after-write",
        base="tests.support.misuse.dlock:ctrl_release_after_write",
        summary="The same spin-acquire lease protocol as the D2 mutant, releasing only after "
        "the critical-section write commits.",
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
        control_id="ctrl-lock-protocol",
        base="tests.support.misuse.dlock:ctrl_lock_protocol",
        summary="The lease protocol: atomic unique-id acquire; a loser backs off, so blind "
        "critical-section writes never race.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-tenant-filtered-browse",
        base="tests.support.misuse.tenancy:ctrl_tenant_filtered_browse",
        summary="The browse filters by the viewer's tenant — a tenant with no rows always sees "
        "zero.",
        adversarial=False,
        clean_band=(0, 32),
    ),
    MisuseControl(
        control_id="ctrl-cache-invalidate-in-tx",
        base="tests.support.misuse.tenancy:ctrl_cache_invalidate_in_tx",
        summary="Source bump and cache update in one transaction — the writer's read-through "
        "always sees its own version.",
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
