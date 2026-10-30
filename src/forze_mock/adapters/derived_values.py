"""Programmable stand-ins for the relation that produces a spec's derived read fields.

A marked derived read field is one the backend's relation produces and the aggregate never
writes, so it travels through no command and the mock has nothing to read it from. Seeding
covers a row a test put there; it cannot cover a row the *workload* created, which is every
row under simulation.

A registered source is what the view would have produced for that row. It is fixture logic
and not a derivation: nothing keeps it in agreement with the view's SQL except a fidelity
test, and keeping it deterministic (a function of the rows, not of a clock or a global
counter) is what keeps a simulation replayable.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Mapping
from typing import Any, cast

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, StrKey
from forze_mock.adapters.analytics_resolvers import MockAnalyticsView

# ----------------------- #

MockDerivedViewSource = Callable[[JsonDict, MockAnalyticsView], Mapping[str, Any]]
"""Produce the derived values for one stored row, reading other rows through a view.

Receives the row as stored and a :class:`MockAnalyticsView`, whose ``rows(spec)`` returns
another spec's stored rows as the reading transaction sees them — the shape an aggregate over
child rows needs (``SUM(child.quantity) WHERE child.parent_id = id``)."""

MockDerivedSource = Callable[[JsonDict], Mapping[str, Any]] | MockDerivedViewSource
"""Produce the derived values for one stored row: ``(row)`` or ``(row, view)``.

Receives the row as stored — and, when it has two required positional parameters, the view
of :data:`MockDerivedViewSource` — and returns a mapping of derived field name to value. Keys the
spec does not declare derived are ignored; values the row already carries win, because a
seeded or staged value is what a test wrote on purpose.
"""

# ....................... #


def as_view_source(source: MockDerivedSource) -> MockDerivedViewSource:
    """*source* in the ``(row, view)`` shape; a ``(row)`` source is wrapped to ignore the view.

    A source that binds one positional argument is row-only, as every source was before views,
    even when it could also bind two (``def f(row, scale=2)``, ``*args``). Only a source with two
    required positional parameters gets the view. A signature that cannot be inspected (a C
    builtin) is row-only too. The shape is read once, here, so a source that fits neither is
    refused where it is registered rather than on the first read that reaches it.

    :raises exc.configuration: when *source* is not callable, is async (the read path does not
        await it), or binds neither ``(row)`` nor ``(row, view)``.
    """

    if not callable(source):
        raise exc.configuration(
            f"A mock derived source must be callable, got {type(source).__name__}.",
            code="mock.derived.source_shape",
        )

    # The type's ``__call__`` covers an instance whose ``__call__`` is async, under any partials.
    target: object = source

    while isinstance(target, functools.partial):
        target = cast("functools.partial[object]", target).func

    if inspect.iscoroutinefunction(target) or inspect.iscoroutinefunction(type(target).__call__):
        raise exc.configuration(
            f"A mock derived source must be synchronous; {source!r} is async, and a read "
            "never awaits it.",
            code="mock.derived.source_shape",
        )

    try:
        signature = inspect.signature(source)

    except (TypeError, ValueError):
        # Nothing to inspect (a C builtin): keep the one shape every source had before views.
        return _ignoring_view(cast("Callable[[JsonDict], Mapping[str, Any]]", source))

    if _binds(signature, 1):
        return _ignoring_view(cast("Callable[[JsonDict], Mapping[str, Any]]", source))

    if _binds(signature, 2):
        return cast("MockDerivedViewSource", source)

    raise exc.configuration(
        f"A mock derived source must take (row) or (row, view); {signature} takes neither.",
        code="mock.derived.source_shape",
    )


def _binds(signature: inspect.Signature, count: int) -> bool:
    try:
        signature.bind(*range(count))

    except TypeError:
        return False

    return True


def _ignoring_view(source: Callable[[JsonDict], Mapping[str, Any]]) -> MockDerivedViewSource:
    return lambda row, _view: source(row)


# ....................... #


@attrs.define(slots=True)
class MockDerivedRegistry:
    """Programmable derived-value sources, keyed by document spec name."""

    _sources: dict[str, MockDerivedViewSource] = attrs.field(factory=dict)

    def on(self, spec: StrKey | str, source: MockDerivedSource) -> MockDerivedRegistry:
        """Register *source* for the document spec named *spec*. Returns self (chainable).

        :raises exc.configuration: when *source* takes neither ``(row)`` nor ``(row, view)``
            (see :func:`as_view_source`).
        """

        self._sources[str(spec)] = as_view_source(source)
        return self

    def source_for(self, spec: str) -> MockDerivedViewSource | None:
        return self._sources.get(spec)
