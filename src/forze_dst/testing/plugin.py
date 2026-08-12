"""Opt-in pytest plugin for DST — ``--dst-seeds`` scaling and the ``dst`` marker.

Enable it in your ``conftest.py``::

    pytest_plugins = ["forze_dst.testing.plugin"]

The :func:`~forze_dst.testing.assert_no_violation` helper works **without** this plugin — it is
a plain assertion. The plugin adds three things on top:

* ``--dst-seeds=N`` (or ini ``dst_seeds``) — override every sweep's seed count, so one test runs
  quick locally and exhaustive in CI with no code change.
* the ``dst`` marker — tag DST tests (``@pytest.mark.dst``) so a suite can select or skip them
  (``pytest -m dst`` / ``-m "not dst"``), e.g. to run the heavy ones nightly only.
* a **clean-run verdict summary** — after the run, one line per clean scenario sweep stating the
  exact exclusion bound it established (``0 violations in N seeds → per-seed detection
  probability < …``) instead of leaving green as an empty claim.

It is **not** auto-loaded: importing the DST package costs roughly a third of a second, so it
stays off until a project opts in rather than taxing every pytest session.
"""

from __future__ import annotations

from typing import Any

from forze_dst.testing._options import DstOptions, active, drain_clean_sweeps, set_active

# ----------------------- #


def pytest_addoption(parser: Any) -> None:
    """Register ``--dst-seeds`` and the ini ``dst_seeds`` default."""

    group = parser.getgroup("dst", "Deterministic Simulation Testing")
    group.addoption(
        "--dst-seeds",
        type=int,
        default=None,
        help="Override every assert_no_violation sweep to this many seeds "
        "(scale one test quick locally / exhaustive in CI).",
    )
    group.addoption(
        "--dst-save-bundle",
        type=str,
        default=None,
        metavar="DIR",
        help="On a failure, drop a portable FailureBundle (seed + full config) into DIR "
        "for CI to keep and replay.",
    )
    # Tri-state: ``None`` means "the CLI said nothing", so the ini value stands. A bare
    # ``store_true`` would make the ini un-overridable — there would be no way to say *off* for
    # one run of a project that turned it on.
    group.addoption(
        "--dst-family-verdict",
        dest="dst_family_verdict",
        action="store_true",
        default=None,
        help="Also print one family-wise-corrected verdict line holding simultaneously across "
        "every clean sweep in the session (off by default: the per-sweep lines are the honest "
        "claim).",
    )
    group.addoption(
        "--no-dst-family-verdict",
        dest="dst_family_verdict",
        action="store_false",
        default=None,
        help="Suppress the family verdict line for this run, overriding ini dst_family_verdict.",
    )
    parser.addini(
        "dst_seeds",
        "Default seed count for DST sweeps (overridden by --dst-seeds).",
        default=None,
    )
    parser.addini(
        "dst_save_bundle",
        "Directory for FailureBundles on failure (overridden by --dst-save-bundle).",
        default=None,
    )
    parser.addini(
        "dst_family_verdict",
        "Print the simultaneous family verdict line (overridden by --dst-family-verdict / "
        "--no-dst-family-verdict).",
        type="bool",
        default=False,
    )


# ....................... #


def pytest_configure(config: Any) -> None:
    """Register the ``dst`` marker and stash the resolved seed override for the helper."""

    config.addinivalue_line(
        "markers",
        "dst: a Deterministic Simulation Testing test (select with -m dst).",
    )

    seeds = config.getoption("--dst-seeds")
    if seeds is None:
        ini = config.getini("dst_seeds")
        seeds = int(ini) if ini else None

    save_bundle = config.getoption("--dst-save-bundle") or config.getini("dst_save_bundle")

    # The CLI wins when it said anything at all — including ``--no-dst-family-verdict``, which is
    # ``False`` rather than "unset" and so must not fall through to the ini.
    family = config.getoption("dst_family_verdict")
    if family is None:
        family = bool(config.getini("dst_family_verdict"))

    set_active(
        DstOptions(seeds=seeds, save_bundle=save_bundle or None, family_verdict=bool(family))
    )


# ....................... #


def pytest_terminal_summary(terminalreporter: Any) -> None:
    """Print one quantitative verdict line per clean scenario sweep the session ran.

    Each line is scoped to its own test (scenario × strategy × oracle set) — the bounds are never
    aggregated across tests, because a combined number would claim more than any single sweep
    established. That refusal is the default and stays; ``--dst-family-verdict`` adds one
    family-wise-corrected line beside it that *is* true of every sweep at once, printed only when
    all share a confidence level (there would otherwise be no single level to state).
    """

    records = drain_clean_sweeps()
    if not records:
        return

    # Local import: the DST facade is heavy (~⅓ s), and a record existing means the helper
    # already loaded it — an empty session never pays the cost.
    from forze_dst.stats import format_clean_verdict, format_family_verdict

    terminalreporter.write_sep("-", "DST clean-run verdicts")
    for record in records:
        verdict = format_clean_verdict(
            record.runs,
            confidence=record.confidence,
            witnessed=record.witnessed,
            declared=record.declared,
            unexercisable=record.unexercisable,
            unaccounted=record.unaccounted,
        )
        terminalreporter.write_line(f"{record.label}: {verdict}")

    options = active()
    levels = {record.confidence for record in records}

    if options is not None and options.family_verdict and len(levels) == 1:
        terminalreporter.write_line(
            format_family_verdict([r.runs for r in records], confidence=levels.pop())
        )


# ....................... #


def pytest_unconfigure(config: Any) -> None:
    """Clear the stashed options at session end (leave no global state behind)."""

    del config
    set_active(None)
