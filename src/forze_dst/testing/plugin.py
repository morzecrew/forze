"""Opt-in pytest plugin for DST — ``--dst-seeds`` scaling and the ``dst`` marker.

Enable it in your ``conftest.py``::

    pytest_plugins = ["forze_dst.testing.plugin"]

The :func:`~forze_dst.testing.assert_no_violation` helper works **without** this plugin — it is
a plain assertion. The plugin adds two things on top:

* ``--dst-seeds=N`` (or ini ``dst_seeds``) — override every sweep's seed count, so one test runs
  quick locally and exhaustive in CI with no code change.
* the ``dst`` marker — tag DST tests (``@pytest.mark.dst``) so a suite can select or skip them
  (``pytest -m dst`` / ``-m "not dst"``), e.g. to run the heavy ones nightly only.

It is **not** auto-loaded: importing the DST package costs roughly a third of a second, so it
stays off until a project opts in rather than taxing every pytest session.
"""

from __future__ import annotations

from typing import Any

from forze_dst.testing._options import DstOptions, set_active

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
    parser.addini(
        "dst_seeds",
        "Default seed count for DST sweeps (overridden by --dst-seeds).",
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

    set_active(DstOptions(seeds=seeds))


# ....................... #


def pytest_unconfigure(config: Any) -> None:
    """Clear the stashed options at session end (leave no global state behind)."""

    del config
    set_active(None)
