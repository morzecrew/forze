"""Fallback provenance — what a hybrid (fallback + real) wiring resolves to.

A registration marked ``fallback=True`` (see :meth:`Deps.plain`/:meth:`Deps.routed`)
declares itself a *background environment*: a real registration for the same key wins
instead of colliding. That relaxation is deliberately observable — merging produces this
report so a hybrid context names which fallback entries were displaced and which ones
still answer calls, rather than leaving either ambient.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, final

import attrs

from forze.base.primitives import MappingConverter, StrKey

from .keys import DepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ShadowedFallback:
    """A fallback registration a non-fallback one displaced during merge."""

    key: DepKey[Any]
    """The dependency key whose fallback entry lost."""

    route: StrKey | None = None
    """The route whose fallback entry lost, or ``None`` for a plain entry."""

    # ....................... #

    def describe(self) -> str:
        """One-line description for reports and logs."""

        where = "plain" if self.route is None else f"route '{self.route}'"

        return f"{self.key.name} ({where})"


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FallbackReport:
    """What a merged :class:`~forze.application.contracts.deps.ProviderStore` owes to
    fallback registrations.

    ``shadowed`` is what a real module took over; ``served_plain``/``served_routes`` is
    what the fallback still answers. :attr:`hybrid` is the one bit worth gating on: a
    store that is *only* fallback (the plain mock context) or *only* real (production)
    is not hybrid and needs no attention.
    """

    shadowed: tuple[ShadowedFallback, ...] = ()
    """Fallback entries dropped in favor of a non-fallback registration."""

    served_plain: frozenset[DepKey[Any]] = frozenset()
    """Keys whose surviving plain entry is fallback-provided. Each also answers **any**
    route that no routed entry covers (``get_provider`` falls back to plain), which is
    what makes a mistyped route reach the fallback instead of failing."""

    served_routes: Mapping[DepKey[Any], frozenset[StrKey]] = attrs.field(
        factory=dict[DepKey[Any], frozenset[StrKey]],
        converter=MappingConverter.frozen,  # type: ignore[misc]
    )
    """Routes whose surviving routed entry is fallback-provided, per key. Frozen: a report
    is a snapshot of one wiring, so a reader cannot edit what it later describes."""

    catch_all: frozenset[DepKey[Any]] = frozenset()
    """The hazard set: :attr:`served_plain` keys a real module **also** registered routes
    for. Those routes resolve to the real adapter, and the fallback silently answers every
    other route — so a mistyped spec name on one of these keys reaches the mock instead of
    failing. The rest of :attr:`served_plain` is simply a plane nobody wired for real."""

    mixed: bool = False
    """Whether the store holds both fallback-marked and non-fallback registrations."""

    # ....................... #

    @property
    def hybrid(self) -> bool:
        """Whether this is a hybrid wiring — a fallback environment composed with real
        modules, either still serving keys (:attr:`mixed`) or displaced by them
        (:attr:`shadowed`). A pure fallback context and a pure real one are both ``False``."""

        return self.mixed or bool(self.shadowed)

    # ....................... #

    @property
    def empty(self) -> bool:
        """Whether nothing at all is fallback-marked."""

        return not self.shadowed and not self.served_plain and not self.served_routes

    # ....................... #

    def served_names(self) -> tuple[str, ...]:
        """Sorted ``key`` / ``key[route]`` labels for everything the fallback still serves."""

        names = [key.name for key in self.served_plain]
        names.extend(
            f"{key.name}[{route}]" for key, routes in self.served_routes.items() for route in routes
        )

        return tuple(sorted(names))

    # ....................... #

    def shadowed_names(self) -> tuple[str, ...]:
        """Sorted descriptions of every displaced fallback entry."""

        return tuple(sorted(entry.describe() for entry in self.shadowed))

    # ....................... #

    def catch_all_names(self) -> tuple[str, ...]:
        """Sorted names of the keys in :attr:`catch_all`."""

        return tuple(sorted(key.name for key in self.catch_all))

    # ....................... #

    def describe(self) -> str:
        """Full multi-line description: what is shadowed, what the fallback still serves,
        and which of those sit behind real routes (the mistyped-route hazard)."""

        shadowed = ", ".join(self.shadowed_names()) or "<none>"
        served = ", ".join(self.served_names()) or "<none>"
        catch_all = ", ".join(self.catch_all_names()) or "<none>"

        return (
            f"shadowed fallbacks: {shadowed}\n"
            f"fallback-served: {served}\n"
            f"catch-all behind real routes: {catch_all}"
        )
