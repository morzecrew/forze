"""Shared pytest-option state — set by the plugin, read by the assertion helper.

Deliberately import-light (no DST facade), so the plugin can register options and stash them
without pulling the heavy ``forze_dst`` package until a helper actually runs a sweep.
"""

from __future__ import annotations

import attrs

# ----------------------- #


@attrs.define(frozen=True, kw_only=True, slots=True)
class DstOptions:
    """Resolved pytest-side DST knobs (the active session's, when the plugin is enabled)."""

    seeds: int | None = None
    """``--dst-seeds`` / ini ``dst_seeds`` — override every sweep to this many seeds, so the same
    test runs quick locally and exhaustive in CI without a code change. ``None`` leaves each
    config's own seed range untouched."""

    save_bundle: str | None = None
    """``--dst-save-bundle`` / ini ``dst_save_bundle`` — directory to drop a portable
    :class:`~forze_dst.artifacts.FailureBundle` into whenever a sweep fails, so CI keeps the
    seed + full config to reproduce it. ``None`` saves nothing."""


# ....................... #


@attrs.define(frozen=True, kw_only=True, slots=True)
class CleanSweep:
    """One clean sweep the session ran: which test, and how many seeds came back clean.

    The raw material for the terminal-summary verdict lines — the *count* is recorded here (this
    module stays import-light); the exclusion bound is computed at render time by the plugin.
    """

    label: str
    """The pytest test id the sweep ran under (or the helper's name when unknown)."""

    runs: int
    """How many seeds the clean sweep ran."""


# ....................... #

_ACTIVE: DstOptions | None = None
_CLEAN_SWEEPS: list[CleanSweep] = []


def set_active(options: DstOptions | None) -> None:
    """Install (or clear) the session's options — called by the plugin's configure hooks.

    Either way the clean-sweep records reset: a new session must not inherit a previous
    in-process session's verdicts (pytester-style nested runs), and unconfigure leaves nothing.
    """

    global _ACTIVE
    _ACTIVE = options  # pyright: ignore[reportConstantRedefinition]
    _CLEAN_SWEEPS.clear()


def active() -> DstOptions | None:
    """The current session's options, or ``None`` when the plugin is not enabled."""

    return _ACTIVE


def record_clean_sweep(record: CleanSweep) -> None:
    """Append one clean sweep for the session's terminal summary (helper-side)."""

    _CLEAN_SWEEPS.append(record)


def drain_clean_sweeps() -> tuple[CleanSweep, ...]:
    """The session's recorded clean sweeps, clearing the buffer (summary-side, called once)."""

    records = tuple(_CLEAN_SWEEPS)
    _CLEAN_SWEEPS.clear()
    return records
