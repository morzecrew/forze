"""Render the campaign evidence charts as light/dark SVG pairs for the docs.

Two figures from the full-protocol JSONL: the Kaplan–Meier detection curves for the depth-2
mutant (the cell where strategy behavior is theoretically interesting), and a forest plot of the
per-seed detection probability with exact 95% intervals for every (mutant, strategy). Colors are
the validated 3-slot categorical palette (all-pairs safe in both modes); text wears text tokens,
never series color; the docs embed the pair via the theme-scoped image convention, and the full
tables remain the canonical data view.

Usage: ``python render_campaign_charts.py <campaign.jsonl> --out pages/docs/dst/_images``
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path

from forze_dst.stats import SurvivalCurve, geometric_p_hat
from tests.support.misuse import CORPUS

STRATEGIES = ("random", "pct-d2", "pct-d3")
KM_MUTANT = "T3-double-torn"

THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "text": "#0b0b0b",
        "muted": "#52514e",
        "grid": "#e7e6e2",
        "series": {"random": "#2a78d6", "pct-d2": "#eb6834", "pct-d3": "#1baf7a"},
    },
    "dark": {
        "surface": "#1a1a19",
        "text": "#ffffff",
        "muted": "#c3c2b7",
        "grid": "#383835",
        "series": {"random": "#3987e5", "pct-d2": "#d95926", "pct-d3": "#199e70"},
    },
}
FONT = 'font-family="Roboto, system-ui, sans-serif"'


def _load(records_path: Path) -> dict[tuple[str, str], list[dict[str, object]]]:
    groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for line in records_path.read_text().splitlines():
        record = json.loads(line)
        if record.get("kind") == "campaign":
            groups[(str(record["mutant_id"]), str(record["strategy"]))].append(record)
    return groups


def _events(records: list[dict[str, object]]) -> tuple[list[int], list[int]]:
    events = [int(r["detection_trial"]) for r in records if r["detection_trial"] is not None]  # type: ignore[arg-type]
    censored = [int(r["trials_run"]) for r in records if r["detection_trial"] is None]  # type: ignore[arg-type]
    return events, censored


def km_chart(
    groups: dict[tuple[str, str], list[dict[str, object]]], theme: Mapping[str, object]
) -> str:
    width, height = 760, 420
    left, right, top, bottom = 64, 150, 56, 48
    plot_w, plot_h = width - left - right, height - top - bottom
    t_max = 250
    series_color: dict[str, str] = theme["series"]  # type: ignore[assignment]

    def sx(t: float) -> float:
        return left + plot_w * min(t, t_max) / t_max

    def sy(s: float) -> float:
        return top + plot_h * (1.0 - s)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" '
        'aria-label="Kaplan-Meier detection curves for the depth-3 mutant">',
        f'<rect width="{width}" height="{height}" fill="{theme["surface"]}" rx="8"/>',
        f'<text x="{left}" y="26" {FONT} font-size="15" font-weight="600" fill="{theme["text"]}">'
        f"Seeds to first detection — {KM_MUTANT} (d=3, N=300 per strategy)</text>",
        f'<text x="{left}" y="44" {FONT} font-size="12" fill="{theme["muted"]}">'
        "P(still undetected) after t trials; every campaign detected before the ceiling</text>",
    ]

    for s in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = sy(s)
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" '
            f'stroke="{theme["grid"]}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{left - 8}" y="{y + 4:.1f}" {FONT} font-size="11" '
            f'fill="{theme["muted"]}" text-anchor="end">{s:.2f}</text>'
        )
    for t in (0, 50, 100, 150, 200, 250):
        x = sx(t)
        parts.append(
            f'<text x="{x:.1f}" y="{top + plot_h + 18}" {FONT} font-size="11" '
            f'fill="{theme["muted"]}" text-anchor="middle">{t}</text>'
        )
    parts.append(
        f'<text x="{left + plot_w / 2:.0f}" y="{height - 10}" {FONT} font-size="11" '
        f'fill="{theme["muted"]}" text-anchor="middle">trials (seeds run)</text>'
    )

    for index, strategy in enumerate(STRATEGIES):
        events, censored = _events(groups[(KM_MUTANT, strategy)])
        curve = SurvivalCurve.fit(events, censored)
        d = [f"M {sx(0):.1f} {sy(1.0):.1f}"]
        for step in curve.steps:
            if step.time > t_max:
                break
            d.append(f"H {sx(step.time):.1f} V {sy(step.survival):.1f}")
        d.append(f"H {sx(t_max):.1f}")
        color = series_color[strategy]
        median = curve.median
        parts.append(
            f'<path d="{" ".join(d)}" fill="none" stroke="{color}" stroke-width="2" '
            f'stroke-linejoin="round"><title>{strategy}: median {median} trials</title></path>'
        )
        # The curves all converge toward zero, so line-end labels would collide — stack them
        # as a fixed legend column instead (identity still never color-alone).
        parts.append(
            f'<text x="{left + plot_w + 10}" y="{top + 16 + index * 20}" {FONT} font-size="12" '
            f'fill="{theme["text"]}">'
            f'<tspan fill="{color}">●</tspan> {strategy} (median {median})</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def forest_chart(
    groups: dict[tuple[str, str], list[dict[str, object]]], theme: Mapping[str, object]
) -> str:
    mutant_ids = [m.mutant_id for m in CORPUS]
    width = 760
    left, right, top, row_h, sub_h = 240, 40, 88, 58, 15
    plot_w = width - left - right
    height = top + row_h * len(mutant_ids) + 44
    series_color: dict[str, str] = theme["series"]  # type: ignore[assignment]

    def sx(p: float) -> float:
        return left + plot_w * p

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" '
        'aria-label="Per-seed detection probability with exact intervals per mutant and strategy">',
        f'<rect width="{width}" height="{height}" fill="{theme["surface"]}" rx="8"/>',
        f'<text x="24" y="28" {FONT} font-size="15" font-weight="600" fill="{theme["text"]}">'
        "Per-seed detection probability p̂ (exact 95% CI, N=300)</text>",
        f'<text x="24" y="46" {FONT} font-size="12" fill="{theme["muted"]}">'
        "Campaign regimes; the full table is the canonical data view</text>",
    ]

    legend_x = 24
    for strategy in STRATEGIES:
        parts.append(
            f'<circle cx="{legend_x + 4}" cy="63" r="4" fill="{series_color[strategy]}"/>'
            f'<text x="{legend_x + 13}" y="67" {FONT} font-size="12" fill="{theme["text"]}">{strategy}</text>'
        )
        legend_x += 90

    for p in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = sx(p)
        parts.append(
            f'<line x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{height - 36}" '
            f'stroke="{theme["grid"]}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{x:.1f}" y="{height - 20}" {FONT} font-size="11" '
            f'fill="{theme["muted"]}" text-anchor="middle">{p:g}</text>'
        )

    for row, mutant_id in enumerate(mutant_ids):
        y0 = top + row * row_h
        parts.append(
            f'<text x="{left - 12}" y="{y0 + row_h / 2 + 4:.1f}" {FONT} font-size="12" '
            f'fill="{theme["text"]}" text-anchor="end">{mutant_id}</text>'
        )
        for index, strategy in enumerate(STRATEGIES):
            events, censored = _events(groups[(mutant_id, strategy)])
            estimate = geometric_p_hat(events, censored)
            y = y0 + 8 + index * sub_h
            color = series_color[strategy]
            title = (
                f"<title>{mutant_id} · {strategy}: p̂={estimate.p_hat:.3f} "
                f"[{estimate.ci.lower:.3f}, {estimate.ci.upper:.3f}]</title>"
            )
            parts.append(
                f'<g>{title}<line x1="{sx(estimate.ci.lower):.1f}" y1="{y}" '
                f'x2="{sx(estimate.ci.upper):.1f}" y2="{y}" stroke="{color}" stroke-width="2"/>'
                f'<circle cx="{sx(estimate.p_hat):.1f}" cy="{y}" r="4" fill="{color}" '
                f'stroke="{theme["surface"]}" stroke-width="2"/></g>'
            )

    parts.append("</svg>")
    return "\n".join(parts)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)

    groups = _load(args.records)
    args.out.mkdir(parents=True, exist_ok=True)

    for mode, theme in THEMES.items():
        (args.out / f"campaign_km_{mode}.svg").write_text(km_chart(groups, theme))
        (args.out / f"campaign_forest_{mode}.svg").write_text(forest_chart(groups, theme))

    print(f"wrote 4 charts to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
