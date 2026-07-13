"""The catalogue: every spec an application binds, and how they relate."""

from __future__ import annotations

from typing import Self, final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec
from .planes import DEFAULT_DISPOSITIONS, plane_of_spec
from .value_objects import (
    PlaneDisposition,
    SpecEdge,
    SpecEdgeKind,
    SpecPlane,
    SpecRef,
    SpecRegistryEntry,
    SpecSource,
)

# ----------------------- #


def _key(entry: SpecRegistryEntry) -> tuple[str, str]:
    return (entry.plane.value, entry.name)


# ....................... #


def spec_ref(spec: BaseSpec) -> SpecRef:
    """Point at a spec by plane and name, without holding it."""

    plane = plane_of_spec(spec)

    if plane is None:
        raise exc.configuration(
            f"{type(spec).__name__} belongs to no inventoried plane and cannot be referenced."
        )

    return SpecRef(plane=plane, name=str(spec.name))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class SpecRegistry:
    """Builder for an application's spec inventory.

    Entries are keyed by ``(plane, name)`` and **never** by the spec object. Two reasons, and
    both bite in practice:

    - ``DocumentSpec`` and ``SearchSpec`` are *unhashable* (their ``write`` mapping is a dict
      and ``fields`` a list), so a ``set`` of specs raises ``TypeError`` rather than merely
      failing to dedupe.
    - Several specs are **rebuilt on every access** — ``StoredFileKitSpec.document`` mints a
      fresh ``DocumentSpec`` per read, and a kit's search-sync wiring re-derives its outbox,
      queue and inbox each time it is asked. Identity dedup would emit four copies of the same
      route; value equality is the only thing that works.

    Re-registering the same ``(plane, name)`` is therefore fine **when the specs are equal** —
    that is the derived-spec case above, not a mistake. Registering a *different* spec under a
    name already taken is a wiring error and fails closed.
    """

    _entries: dict[tuple[str, str], SpecRegistryEntry] = attrs.field(factory=dict, init=False)
    _edges: set[SpecEdge] = attrs.field(factory=set, init=False)

    # ....................... #

    def register(
        self,
        *specs: BaseSpec,
        source: SpecSource = SpecSource.AUTHOR,
        disposition: PlaneDisposition | None = None,
    ) -> Self:
        """Catalogue *specs*, inferring each one's plane from its type.

        A spec whose type belongs to no inventoried plane (a ``ProcedureSpec``, an
        ``HttpServiceSpec`` — compute and I/O, holding no state of their own) is rejected
        rather than silently dropped: the caller thought it was contributing something.
        """

        for spec in specs:
            plane = plane_of_spec(spec)

            if plane is None:
                raise exc.configuration(
                    f"{type(spec).__name__} is not an inventoried plane and cannot be "
                    f"registered (it binds no state, derived data, or in-flight work)."
                )

            self.register_entry(
                SpecRegistryEntry(
                    plane=plane,
                    name=str(spec.name),
                    spec=spec,
                    disposition=disposition or DEFAULT_DISPOSITIONS[plane],
                    source=source,
                )
            )

        return self

    # ....................... #

    def register_entry(self, entry: SpecRegistryEntry) -> Self:
        """Catalogue one fully-formed entry."""

        existing = self._entries.get(_key(entry))

        if existing is None:
            self._entries[_key(entry)] = entry
            return self

        if existing.spec == entry.spec:
            # The same spec, re-derived. Benign, and unavoidable: several specs are rebuilt
            # per access rather than held.
            return self

        raise exc.configuration(
            f"Two different specs are registered as {entry.ref.label()!r} "
            f"(one from {existing.source.value}, one from {entry.source.value}). "
            f"A route name identifies exactly one spec."
        )

    # ....................... #

    def link(self, kind: SpecEdgeKind, *, source: BaseSpec, target: BaseSpec) -> Self:
        """Record a relationship neither spec can express on its own.

        Both ends must end up catalogued — :func:`reconcile_specs` rejects a dangling edge.
        """

        self._edges.add(SpecEdge(kind=kind, source=spec_ref(source), target=spec_ref(target)))

        return self

    # ....................... #

    def merge(self, *others: SpecRegistry) -> Self:
        """Fold other registries into this one, with the same collision rule."""

        for other in others:
            for entry in other._entries.values():
                self.register_entry(entry)

            self._edges.update(other._edges)

        return self

    # ....................... #

    def freeze(self) -> FrozenSpecRegistry:
        """Seal the catalogue into its immutable, deterministically-ordered form."""

        return FrozenSpecRegistry(
            entries=tuple(sorted(self._entries.values(), key=_key)),
            edges=tuple(sorted(self._edges, key=lambda edge: edge.label())),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenSpecRegistry:
    """An application's sealed spec inventory."""

    entries: tuple[SpecRegistryEntry, ...]
    """Every catalogued spec, ordered by ``(plane, name)``."""

    edges: tuple[SpecEdge, ...]
    """Every catalogued relationship, ordered by label."""

    # ....................... #

    def of_plane(self, plane: SpecPlane) -> tuple[SpecRegistryEntry, ...]:
        """Every entry on one plane."""

        return tuple(entry for entry in self.entries if entry.plane is plane)

    # ....................... #

    def find(self, plane: SpecPlane, name: str) -> SpecRegistryEntry | None:
        """One entry, or ``None`` when the name is not catalogued on that plane."""

        return next(
            (entry for entry in self.entries if entry.plane is plane and entry.name == name),
            None,
        )

    # ....................... #

    def of_disposition(self, disposition: PlaneDisposition) -> tuple[SpecRegistryEntry, ...]:
        """Every entry an export must treat the same way — the doctrine, made queryable."""

        return tuple(entry for entry in self.entries if entry.disposition is disposition)

    # ....................... #

    def edges_of(self, kind: SpecEdgeKind) -> tuple[SpecEdge, ...]:
        """Every relationship of one kind."""

        return tuple(edge for edge in self.edges if edge.kind is kind)
