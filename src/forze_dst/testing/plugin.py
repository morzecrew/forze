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

from forze_dst.testing._options import DstOptions, drain_clean_sweeps, set_active

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

    set_active(DstOptions(seeds=seeds, save_bundle=save_bundle or None))


# ....................... #


def pytest_terminal_summary(terminalreporter: Any) -> None:
    """Print one quantitative verdict line per clean scenario sweep the session ran.

    Each line is scoped to its own test (scenario × strategy × oracle set) — the bounds are never
    aggregated across tests, because a combined number would claim more than any single sweep
    established.
    """

    records = drain_clean_sweeps()
    if not records:
        return

    # Local import: the DST facade is heavy (~⅓ s), and a record existing means the helper
    # already loaded it — an empty session never pays the cost.
    from forze_dst.stats import format_clean_verdict

    terminalreporter.write_sep("-", "DST clean-run verdicts")
    for record in records:
        terminalreporter.write_line(f"{record.label}: {format_clean_verdict(record.runs)}")


# ....................... #


def pytest_unconfigure(config: Any) -> None:
    """Clear the stashed options at session end (leave no global state behind)."""

    del config
    set_active(None)
