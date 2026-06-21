"""Back-compat shim — the test-context builders now live in :mod:`forze.testing` (shipped in core).

Kept so existing ``from tests.support.execution_context import …`` imports keep working; new tests
should import from ``forze.testing`` directly.
"""

from __future__ import annotations

from forze.testing import (
    context_from_deps,
    context_from_modules,
    frozen_deps_from_deps,
    frozen_deps_from_modules,
)

__all__ = [
    "context_from_deps",
    "context_from_modules",
    "frozen_deps_from_deps",
    "frozen_deps_from_modules",
]
