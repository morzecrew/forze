"""Quantitative clean-run verdicts — what a green sweep actually excludes.

A clean sweep of ``S`` independent seeds is not "no bugs"; it is an *exclusion bound*. If the
per-seed probability of detecting a defect is ``p``, all ``S`` seeds come back clean with
probability ``(1 - p) ** S`` — so the largest ``p`` still consistent with an all-clean outcome
at confidence ``γ`` is::

    p_upper = 1 - (1 - γ) ** (1 / S)

the exact zero-event Clopper–Pearson upper limit, whose familiar mnemonic is the *rule of three*
(``≈ 3 / S`` at 95%). :func:`detection_upper_bound` computes the exact form;
:func:`format_clean_verdict` renders the one locked sentence every clean-run surface prints
(:meth:`~forze_dst.artifacts.sweep.SweepResult.format`,
:meth:`~forze_dst.oracle.confidence.ConfidenceReport.format`,
:meth:`~forze_dst.oracle.coverage.CoverageStats.format`, the CLI) — scope clause included, so
the number never travels without the claim it is scoped to.

The bound is deliberately narrow. It speaks only about the configured scenario × strategy ×
oracle set, under independent seeds. It never claims the absence of bugs: a defect this workload
cannot express, an oracle blind to the violation, or a strategy that cannot reach the triggering
interleaving all live outside it.
"""

from __future__ import annotations

# ----------------------- #


def detection_upper_bound(runs: int, *, confidence: float = 0.95) -> float:
    """The exact upper bound on per-seed detection probability after *runs* clean seeds.

    Zero-event Clopper–Pearson: ``1 - (1 - confidence) ** (1 / runs)`` — the largest per-seed
    detection probability under which an all-clean sweep of *runs* independent seeds is still
    plausible at the given *confidence*. ``3 / runs`` (the rule of three) is its 95% mnemonic;
    this returns the exact value.
    """

    if runs < 1:
        raise ValueError(f"runs must be >= 1, got {runs}")

    if not 0.0 < confidence < 1.0:
        raise ValueError(f"confidence must be in (0, 1), got {confidence}")

    return 1.0 - (1.0 - confidence) ** (1.0 / runs)


# ....................... #


def _render_probability(bound: float) -> str:
    """Percent with two decimals; scientific below display resolution (so it never shows 0.00%)."""

    return f"{bound:.2%}" if bound >= 0.0005 else f"{bound:.1e}"


# ....................... #


def _render_confidence(confidence: float) -> str:
    """Percent with trailing zeros trimmed, so 0.95 → ``95%`` but 0.999 → ``99.9%``, never ``100%``."""

    return f"{confidence:.4%}".removesuffix("%").rstrip("0").rstrip(".") + "%"


# ....................... #


def format_clean_verdict(runs: int, *, confidence: float = 0.95) -> str:
    """The locked one-line verdict a clean run prints instead of a bare "passed".

    One shared sentence — bound plus scope clause — so every surface (sweep, confidence report,
    coverage report, CLI) states exactly the same claim and never a stronger one.
    """

    bound = detection_upper_bound(runs, confidence=confidence)
    plural = "seeds" if runs != 1 else "seed"

    return (
        f"0 violations in {runs} {plural} → per-seed detection probability "
        f"< {_render_probability(bound)} ({_render_confidence(confidence)}, exact) "
        "for this scenario × strategy × oracle set (independent seeds)"
    )
