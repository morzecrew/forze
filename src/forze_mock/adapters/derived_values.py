"""Programmable stand-ins for the relation that produces a spec's derived read fields.

A marked derived read field is one the backend's relation produces and the aggregate never
writes, so it travels through no command and the mock has nothing to read it from. Seeding
covers a row a test put there; it cannot cover a row the *workload* created, which is every
row under simulation.

A registered source is what the view would have produced for that row. It is fixture logic
and not a derivation: nothing recomputes it when the source row changes, and keeping it
deterministic (a function of the row, not of a clock or a global counter) is what keeps a
simulation replayable.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import attrs

from forze.base.primitives import JsonDict, StrKey

# ----------------------- #

MockDerivedSource = Callable[[JsonDict], Mapping[str, Any]]
"""Produce the derived values for one stored row.

Receives the row as stored and returns a mapping of derived field name to value. Keys the
spec does not declare derived are ignored; values the row already carries win, because a
seeded or staged value is what a test wrote on purpose.
"""


@attrs.define(slots=True)
class MockDerivedRegistry:
    """Programmable derived-value sources, keyed by document spec name."""

    _sources: dict[str, MockDerivedSource] = attrs.field(factory=dict)

    def on(self, spec: StrKey | str, source: MockDerivedSource) -> MockDerivedRegistry:
        """Register *source* for the document spec named *spec*. Returns self (chainable)."""

        self._sources[str(spec)] = source
        return self

    def source_for(self, spec: str) -> MockDerivedSource | None:
        return self._sources.get(spec)
