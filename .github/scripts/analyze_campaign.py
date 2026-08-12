"""Append the p̂-versus-PCT-bound analysis to a campaign summary — the W3 flagship table.

Reads the raw campaign JSONL and the corpus registry, groups measured per-seed detection
probabilities by the mutants' mechanically-derived depth labels, and compares each PCT strategy's
p̂ against the PCT guarantee ``p >= 1/(n * k^(d-1))``. ``n`` and ``k`` come from the records'
per-run schedule profiles (distinct contending tasks; realized ordering-choice ticks), folded per
cell as maxima; records that predate the instrumentation fall back to the structural estimates
(workload concurrency; the ``steps`` draw range) with the fallback stated, never silent.

Two multiplicity guards sit on top of that comparison:

* **Family-wise control.** The scan checks every cell at once and reports one ``Bound violations``
  count, so per-cell 95% is not family-wise 95% — at ~15 cells a spurious flag is likelier than
  not. Each cell's interval is Šidák-corrected to the number of cells actually scanned, and both
  levels are stated in the generated table.
* **The flip margin.** ``p̂_sched = p̂ / p_trigger`` propagates uncertainty through ``p̂`` and
  through nothing else; ``p_trigger`` is a structural constant with none attached. Rather than
  invent a perturbation band, each cell reports the exact factor by which ``p_trigger`` would have
  to be wrong for that cell's verdict to flip.

Usage: ``python analyze_campaign.py <campaign.jsonl> --summary <campaign_full.md>``
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import cast

from forze_dst.stats import flip_margin, geometric_p_hat, sidak_level
from tests.support.misuse import CORPUS

K_ESTIMATE = 50  # the PCT `steps` parameter — change points spread over at most this many ticks
FAMILY_CONFIDENCE = 0.95  # held across the whole scan, not per cell


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args(argv)

    mutants = {m.mutant_id: m for m in CORPUS}

    def trigger_probability(mutant_id: str) -> float | None:
        """P(the workload carries the triggering opportunity), or None = bound not applicable."""

        if mutant_id in ("M1-dual-write-shipment", "I3-ack-before-processing"):
            return None  # fault lottery: the trigger is the crash stream, not the schedule
        if mutant_id in ("N1-drop-tenant-predicate", "D4-unmerged-remote-hlc"):
            return None  # workload-order lottery (put-before-browse / emit-before-relay)

        mutant = mutants[mutant_id]
        explore = mutant.campaign_explore or mutant.killing.explore or {}
        if mutant_id == "T4-weakened-oncall":
            # The skew needs a same-rota AND distinct-doctor concurrent pair: 1/(2·pool).
            return 1.0 / (2.0 * int(cast("int", explore["pool"])))
        if "pool" in explore:
            return 1.0 / float(int(explore["pool"]))
        if mutant_id in ("T3-torn-activation", "T3-double-torn"):
            return 0.5  # P(one provision + one serve in 2 draws)
        return 1.0  # trigger certain: deterministic / every-run opportunity

    groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for line in args.records.read_text().splitlines():
        record = json.loads(line)
        if record.get("kind") == "campaign":
            groups[(str(record["mutant_id"]), str(record["strategy"]))].append(record)

    # Pass one: which cells the scan actually covers. The Šidák correction divides by that count,
    # so no interval may be computed before it is known.
    excluded: list[str] = []
    cells: list[tuple[str, str, list[dict[str, object]], float]] = []

    for (mutant_id, strategy), records in sorted(groups.items()):
        if not strategy.startswith("pct"):
            continue

        if int(strategy.rsplit("d", 1)[-1]) < mutants[mutant_id].depth:
            continue  # the guarantee only speaks for parameter >= depth

        p_trigger = trigger_probability(mutant_id)
        if p_trigger is None:
            if mutant_id not in excluded:
                excluded.append(mutant_id)
            continue

        cells.append((mutant_id, strategy, records, p_trigger))

    per_cell = sidak_level(FAMILY_CONFIDENCE, max(1, len(cells)))

    # With no cell to scan there is no family to correct, and the paragraph below would state a
    # level over zero comparisons that the code did not apply.
    multiplicity = (
        [
            "",
            (
                f"**Multiplicity.** The scan checks {len(cells)} cells and reports one violation "
                "count, so"
            ),
            "a per-cell 95% interval would not be a 95% claim about the family — under the null that",
            "the bound holds everywhere, the chance of at least one spurious flag grows past a coin",
            "flip by ~15 cells, and a false alarm here sends a reviewer off to re-derive a correct",
            f"depth label. Each interval below is therefore computed at **{per_cell:.4%} per cell**",
            (
                f"(Šidák over {len(cells)}), holding **{FAMILY_CONFIDENCE:.0%} family-wise** "
                "across the scan."
            ),
        ]
        if cells
        else []
    )

    lines = [
        "",
        "## p̂ versus the PCT bound (W3)",
        "",
        "PCT with depth parameter ≥ d guarantees, **per trigger-carrying execution**, a schedule-",
        "detection probability ≥ `1/(n·k^(d−1))`. The measured per-seed p̂ is a *product*:",
        "p(workload carries the trigger) × p(schedule realizes it) — so the bound is compared",
        "against the conditional `p̂_sched = p̂ / p_trigger`, with `p_trigger` taken from the",
        "recorded regime structure (the collision pool; the two-rule workload mix). Mutants whose",
        "trigger is a *fault* lottery (crash stream) or an uninstrumented workload-order lottery",
        "are excluded — the theorem does not speak about them.",
        "",
        "`n` and `k` are **measured per run** (distinct contending tasks; realized",
        "ordering-choice ticks), folded per cell as maxima — the largest observed contention",
        "gives the lowest, most conservative floor. The formal bound uses the PCT draw range",
        f"`steps={K_ESTIMATE}` for `k` (the guarantee is over the range the change points are",
        "*drawn* from, not the schedule that happened); the **k-tuned floor** column restates",
        "the same guarantee had `steps` been set to the measured schedule length — the honest",
        "decomposition of any looseness into draw-range slack versus residual conservatism.",
        "A cell whose records predate the instrumentation falls back to the structural",
        "estimates (workload concurrency; the draw range) and says so.",
        *multiplicity,
        "",
        "**Flip margin.** Uncertainty is propagated through `p̂` and through nothing else:",
        "`p_trigger` is a structural constant, several of its values exact combinatorics, but all",
        "of them derived from reviewed reasoning rather than measured. Respect holds iff",
        "`p_trigger ≤ p̂_upper / bound`, so each cell carries the exact factor `F` by which",
        "`p_trigger` would have to be understated for that cell's verdict to flip — no arbitrary",
        "perturbation band to calibrate. A cell at `F = 40×` is immune to any plausible derivation",
        "error; one at `F = 1.2×` is a single reviewed assumption away from a false alarm. Where",
        "the flip would need `p_trigger > 1` it is **unreachable**, reported as such rather than as",
        "a meaningless factor.",
        "",
        "| mutant | d | strategy | n | k | p̂ per seed | p_trigger | p̂_sched | bound | respected | flip margin | k-tuned floor |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    violations = 0
    for mutant_id, strategy, records, p_trigger in cells:
        mutant = mutants[mutant_id]

        measured = all(r.get("max_tasks") is not None for r in records)
        if measured:
            n = max(1, *(int(cast("int", r["max_tasks"])) for r in records))
            k = max(1, *(int(cast("int", r["max_choice_steps"])) for r in records))
            n_cell, k_cell = str(n), str(k)
        else:
            explore = mutant.campaign_explore or mutant.killing.explore or {}
            n = max(1, int(cast("int", explore["concurrency"])))
            k = K_ESTIMATE
            n_cell, k_cell = f"{n} (est.)", f"≤{K_ESTIMATE} (est.)"

        events = [r["detection_trial"] for r in records if r["detection_trial"] is not None]
        censored = [r["trials_run"] for r in records if r["detection_trial"] is None]
        estimate = geometric_p_hat(events, censored, confidence=per_cell)  # type: ignore[arg-type]

        # The formal guarantee draws change points over `steps`; the tuned floor is the same
        # theorem had `steps` matched the measured schedule length.
        bound = 1.0 / (n * (K_ESTIMATE ** (mutant.depth - 1)))
        tuned = 1.0 / (n * (k ** (mutant.depth - 1)))
        p_sched = min(1.0, estimate.p_hat / p_trigger)
        p_sched_upper = min(1.0, estimate.ci.upper / p_trigger)
        respected = p_sched_upper >= bound
        if not respected:
            violations += 1

        margin = flip_margin(observed_upper=estimate.ci.upper, bound=bound, trigger=p_trigger)
        margin_cell = "unreachable" if not margin.reachable else f"{margin.factor:.1f}×"

        lines.append(
            f"| `{mutant_id}` | {mutant.depth} | {strategy} | {n_cell} | {k_cell} "
            f"| {estimate.p_hat:.3f} | {p_trigger:.3f} | {p_sched:.2f} | {bound:.4f} "
            f"| {'yes' if respected else '**NO**'} | {margin_cell} "
            f"| {tuned:.4f} ({p_sched / tuned:.0f}× loose) |"
        )

    lines += [
        "",
        (
            "Excluded from the bound comparison (trigger is not a schedule lottery): "
            f"{', '.join(f'`{m}`' for m in excluded) or 'none'}."
        ),
        "",
        (
            f"**Bound violations: {violations}** "
            f"(family-wise {FAMILY_CONFIDENCE:.0%} over {len(cells)} cells)."
        ),
        "",
        "Reading: for every depth-1 cell the conditional schedule probability sits at ≈ 1 — once",
        "the workload carries the trigger, essentially any schedule realizes it, consistent with",
        "d=1 meaning zero ordering constraints (for d=1 the bound is `1/n` and `k` drops out).",
        "The depth-2 cells are where the bound does real work, and the measured `k` decomposes",
        "their looseness: the formal floor divides by the draw range",
        f"(`steps={K_ESTIMATE}`), but the realized schedules are far shorter — the k-tuned",
        "floor shows how much of the gap is draw-range slack (recoverable by setting `steps`",
        "to the measured schedule length) versus PCT's residual conservatism. A violation",
        "anywhere would have meant a wrong depth label or wrong n/k accounting — the first",
        "(unconditioned) pass of this analysis produced exactly such false violations and was",
        "corrected to the conditional form above.",
        "",
        "`n` and `k` carry no interval on purpose. They are per-run measurements folded per cell",
        "as **maxima** — a biased extreme-order statistic, but biased toward the lowest, most",
        "conservative floor, which is the direction that cannot manufacture a violation. The",
        "absence of an interval there is a decision, not an oversight.",
        "",
    ]

    with args.summary.open("a") as handle:
        handle.write("\n".join(lines))

    print(f"appended W3 analysis ({sum(1 for l in lines if l.startswith('| `'))} rows, {violations} violations)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
