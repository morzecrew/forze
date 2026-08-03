"""Compatibility helpers for optional ``forze_mock`` submodules."""

from __future__ import annotations

from importlib.util import find_spec

# ----------------------- #


def require_seeding() -> None:
    """Raise a clear error when the generator ``forze_mock.seeding`` needs is missing.

    Seeding gates on the ``polyfactory`` import rather than owning an extra of its own, so
    anyone who already installed ``forze[dst]`` gets it for free and nothing self-references
    an extra. Fixtures-only plans still need it: the gate is on the module, not the call.
    """

    if find_spec("polyfactory") is None:
        raise RuntimeError(
            "forze_mock.seeding requires polyfactory — install it with 'forze[dst]' "
            "(or add polyfactory directly)"
        )
