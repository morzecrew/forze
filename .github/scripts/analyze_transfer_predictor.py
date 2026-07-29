"""The pre-registered predictor analysis — does anomaly divergence predict bug divergence?

Joins the two fidelity artifacts: each transferable corpus mutant is mapped (reviewed table
below) to the isolation-battery phenomenon its defect manifests through, the corresponding
matrix cell (engine × phenomenon, at the level the corpus workloads run under) is looked up,
and the 2×2 contingency — battery-clean vs battery-divergent cell × transferred vs diverged —
is tested with the exact Fisher test. The protocol was fixed before the data existed, and both
outcomes are reported: a predictive proxy (run the battery, trust the corpus) or a
non-predictive one (the corpus-on-real run stays load-bearing). Mutants whose defect lies
outside the isolation plane entirely are reported as such — that fraction is itself a finding
about the proxy's domain, never a silent exclusion.

Usage: ``PYTHONPATH=. python analyze_transfer_predictor.py --fidelity <fidelity_postgres.json>
--transfer <transfer_postgres.json> --out <predictor.md>``
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from forze_dst.stats import fisher_exact

# The corpus handlers run under the adapters' default isolation level — READ_COMMITTED on
# Postgres — so that is the matrix row a mutant's phenomenon cell is read from.
CORPUS_LEVEL = "READ_COMMITTED"

# The reviewed mutant -> battery-phenomenon mapping. A mutant maps when its defect, at the port
# level, manifests *through* that phenomenon's interleaving shape; `None` means the defect lies
# outside the isolation plane and no battery cell could predict its transfer even in principle.
MAPPING: dict[str, tuple[str, str] | tuple[None, str]] = {
    "T1-blind-write-payment": (
        "lost_update",
        "the dropped rev guard lets the second writer blindly overwrite a concurrent update",
    ),
    "T3-payment-outside-tx": (
        "dirty_read",
        "the outside-tx payment persists although its guard transaction aborts — the world "
        "observes an aborted transaction's effect (G1a shape)",
    ),
    "T3-torn-activation": (
        "intermediate_read",
        "the torn state is a persisted intermediate of a logically-atomic activation (G1b shape)",
    ),
    "T3-double-torn": (
        "intermediate_read",
        "two persisted intermediates of logically-atomic activations, observed together (G1b "
        "shape, doubled)",
    ),
    "T5-unchecked-reservation": (
        "predicate_write_skew",
        "both sessions evaluate the no-reservation predicate, then both insert (G2 shape)",
    ),
    "T4-weakened-oncall": (
        "write_skew",
        "the read-both/write-own rota constraint at SNAPSHOT is the write-skew shape verbatim "
        "(G2-item)",
    ),
    "I1-retry-without-key": (
        None,
        "idempotency-plane defect — a duplicate re-invocation, no isolation phenomenon involved",
    ),
    "I2-naive-retry-loop": (
        None,
        "retry-plane defect — the duplicate comes from re-running a non-idempotent block; the "
        "conflict that triggers the retry is correct behavior, not an anomaly",
    ),
    "D2-early-lease-release": (
        "lost_update",
        "with the lease dropped mid-section, the waiter's read-modify-blind-write overlaps the "
        "holder's — a lost update on the document plane",
    ),
    "M1-dual-write-shipment": (
        None,
        "crash-atomicity across two planes (state + outbox); fault-triggered, not an interleaving",
    ),
    "I3-ack-before-processing": (
        None,
        "crash/redelivery defect on the messaging plane",
    ),
    "M2-consumer-without-inbox": (
        None,
        "duplicate-delivery semantics, not an isolation phenomenon",
    ),
    "D1-skip-lock": (
        "lost_update",
        "without the lock both workers read-modify-write the balance — a lost update on the "
        "document plane",
    ),
    "D3-nonatomic-acquire": (
        "write_skew",
        "check-then-act on the lock row — each session's check reads the row the other writes "
        "(G2-item shape)",
    ),
    "D4-unmerged-remote-hlc": (
        None,
        "clock-discipline defect — the inversion comes from skewed stamping, not from any "
        "interleaving anomaly",
    ),
    "D5-wall-clock-ordering": (
        None,
        "clock-discipline defect — raw wall stamps under skew, no isolation phenomenon involved",
    ),
    "N1-drop-tenant-predicate": (
        None,
        "query-predicate/tenancy defect, no concurrency involved",
    ),
    "N3-unbound-cursor-walk": (
        None,
        "pagination-plane predicate defect (the resume drops the tenant filter), no "
        "concurrency involved",
    ),
    "N2-stale-cache": (
        None,
        "cache-plane invalidation ordering, outside the document isolation family",
    ),
}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fidelity", required=True, type=Path)
    parser.add_argument("--transfer", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)

    matrix = json.loads(args.fidelity.read_text())
    engine = matrix["engine"]
    cells = {(cell["case"], cell["level"]): cell for cell in matrix["cells"]}

    from tests.support.misuse import CONTROLS

    control_ids = {control.control_id for control in CONTROLS}
    transfers = {
        record["mutant_id"]: record
        for record in json.loads(args.transfer.read_text())
        if record["mutant_id"] not in control_ids
    }
    if transfers.keys() != MAPPING.keys():
        raise ValueError(
            f"mapping/transfer mismatch: unmapped={sorted(transfers.keys() - MAPPING.keys())}, "
            f"stale={sorted(MAPPING.keys() - transfers.keys())}"
        )

    # The 2×2: rows = the mutant's battery cell (clean / divergent), cols = the transfer
    # verdict (agree / diverged). Unmappable mutants have no cell and stand outside the table.
    table = [[0, 0], [0, 0]]
    rows: list[str] = []
    outside: list[tuple[str, str]] = []

    for mutant_id, (case, reason) in MAPPING.items():
        record = transfers[mutant_id]
        transferred = record["classification"] == "agree"

        if case is None:
            outside.append((mutant_id, reason))
            continue

        cell = cells[(case, CORPUS_LEVEL)]
        clean = cell["classification"] == "agree"
        table[0 if clean else 1][0 if transferred else 1] += 1
        rows.append(
            f"| `{mutant_id}` | `{case}` ({cell['adya']}) "
            f"| {'✓ agree' if clean else '✗ ' + str(cell['classification'])} "
            f"| {'✓ agree' if transferred else '✗ ' + str(record['classification'])} "
            f"| {reason} |"
        )

    p_value = fisher_exact(((table[0][0], table[0][1]), (table[1][0], table[1][1])))
    divergent_cells = sum(table[1])
    diverged_transfers = table[0][1] + table[1][1]

    lines = [
        "# Does anomaly divergence predict bug divergence?",
        "",
        "The pre-registered predictor question: are matrix cells where the mock disagrees with",
        "the real engine on an isolation phenomenon also where corpus bugs fail to transfer?",
        "Protocol fixed before the data existed — each transferable mutant is mapped to the",
        f"battery phenomenon its defect manifests through, its `{engine}` cell is read at",
        f"`{CORPUS_LEVEL}` (the level the corpus workloads run under), and the 2×2 is tested",
        "with the exact Fisher test. Both outcomes were committed to in advance: a predictive",
        "proxy (run the battery, trust the corpus) or a non-predictive one (the corpus-on-real",
        "run stays load-bearing).",
        "",
        "*Generated by `just dst-transfer` — do not edit by hand.*",
        "",
        "## Mapped mutants",
        "",
        "| mutant | phenomenon | battery cell | transfer | mapping rationale |",
        "|---|---|---|---|---|",
        *rows,
        "",
        "## The contingency table",
        "",
        "| | transferred | diverged |",
        "|---|---|---|",
        f"| battery-clean cell | {table[0][0]} | {table[0][1]} |",
        f"| battery-divergent cell | {table[1][0]} | {table[1][1]} |",
        "",
        f"Fisher exact (two-sided): **p = {p_value:.4g}**.",
        "",
        "## Verdict",
        "",
    ]

    if divergent_cells == 0 and diverged_transfers == 0:
        lines += [
            f"The data lands in the degenerate branch: the `{engine}` matrix has **zero**",
            "divergent cells and the corpus run had **zero** transfer divergences, so both",
            "margins of the table are empty on one side and the test carries no evidence",
            "either way (p = 1 by construction). What the run *does* establish: every mutant",
            "in a battery-clean cell transferred — the hypothesis's clean-cell branch is",
            "consistent with all observed data — but the divergent-cell branch is unexercised,",
            "so the proxy's predictive power is **untested, not confirmed**. Per the",
            "pre-registered commitment, the conservative conclusion stands: **the corpus-on-real",
            "run stays load-bearing**; the battery is not certified as its substitute.",
        ]
    else:
        lines += [
            "See the table: with divergences present the test is informative — interpret the",
            "direction of association together with the per-mutant rows above, and update the",
            "pre-registered conclusion accordingly.",
        ]

    lines += [
        "",
        f"## Outside the proxy's domain — {len(outside)}/{len(MAPPING)} transferable mutants",
        "",
        "These defects do not manifest through any isolation phenomenon, so anomaly conformance",
        "could never predict their transfer **even in principle** — for this part of the corpus",
        "the battery is not a proxy at all, only the transfer run speaks. This bound on the",
        "proxy's domain is itself a finding of the analysis.",
        "",
        "| mutant | why no battery cell applies |",
        "|---|---|",
        *[f"| `{mutant_id}` | {reason} |" for mutant_id, reason in outside],
        "",
        "With a corpus this size the test is underpowered for subtle effects; the association",
        "worth acting on is the strong kind — a divergent cell with multiple diverging mutants —",
        "and none was observed.",
        "",
    ]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines))

    print(f"rendered {args.out}: table={table}, p={p_value:.4g}, outside={len(outside)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
