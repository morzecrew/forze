"""Programmable per-resource sources modelling parametrized reads over ``MockState``.

The mock has no parametrized view to read session settings from, so a registered source models
what such a relation would yield for a given set of bound query parameters. The document DSL
(filter / sort / paginate / project) then composes over the source's rows exactly as it does over
stored documents — keeping the mock a faithful differential oracle for the real backend.
"""

from __future__ import annotations

from typing import Callable, Sequence

import attrs
from pydantic import BaseModel

from forze.base.primitives import JsonDict, StrKey
from forze_mock.state import MockState

# ----------------------- #

MockQueryParamsSource = Callable[[BaseModel, MockState], Sequence[BaseModel | JsonDict]]
"""Produce the rows a parametrized read yields for the bound *params* and current ``MockState``.

Rows may be read-model instances or plain mappings; the adapter normalizes them to mappings and
applies the document DSL on top."""


@attrs.define(slots=True)
class MockQueryParamsRegistry:
    """Programmable parametrized-read sources, keyed by document route (spec) name."""

    _sources: dict[str, MockQueryParamsSource] = attrs.field(factory=dict)

    def on(
        self,
        route: StrKey | str,
        source: MockQueryParamsSource,
    ) -> MockQueryParamsRegistry:
        """Register *source* for document *route*. Returns self (chainable)."""

        self._sources[str(route)] = source
        return self

    def source_for(self, route: str) -> MockQueryParamsSource | None:
        return self._sources.get(route)
