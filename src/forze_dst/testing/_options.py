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


# ....................... #

_ACTIVE: DstOptions | None = None


def set_active(options: DstOptions | None) -> None:
    """Install (or clear) the session's options — called by the plugin's configure hooks."""

    global _ACTIVE
    _ACTIVE = options  # pyright: ignore[reportConstantRedefinition]


def active() -> DstOptions | None:
    """The current session's options, or ``None`` when the plugin is not enabled."""

    return _ACTIVE
