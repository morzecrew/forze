"""What to seed, and how much of it is left to chance."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


def _require_countable(count: int, what: str) -> None:
    """Refuse a negative generation count, on any plane.

    A negative count is not a smaller seed — ``range(-1)`` is empty, so it silently means
    "generate nothing" and a plan that asked for rows quietly produces none.
    """

    if count < 0:
        raise exc.configuration(f"Seed count for '{what}' must not be negative")


# ....................... #

DEFAULT_SEED_INSTANT = datetime(2026, 1, 1, tzinfo=UTC)
"""The clock a seed runs under by default.

Freezing it is what makes the whole seed reproducible: ids and timestamps are minted by the
*write path*, not by the seeder, so pinning the seeded values alone would still leave every
``id``, ``created_at`` and ``last_update_at`` different on the next run. Pass
``instant=None`` for wall-clock timestamps and give up byte-identical output.
"""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SpecSeed:
    """How much data one document spec gets, and how plausible it has to look."""

    spec: DocumentSpec[Any, Any, Any, Any]
    """The spec to fill. Must be writable — a seed goes through its ``create_cmd``."""

    count: int = 0
    """How many rows to **generate** on top of :attr:`fixtures`."""

    fixtures: tuple[Mapping[str, Any], ...] = ()
    """Hand-written rows, applied verbatim (after :attr:`overrides`).

    Generated values are shape-correct and implausible — a screen full of ``string_a7f3``
    reads as a broken UI, not as seeded data. Fixtures are how a demo or a design review
    gets rows worth looking at; generation is how it gets *volume*.
    """

    overrides: Mapping[str, Any] = attrs.field(factory=dict[str, Any])
    """Field values forced on every row of this spec, fixtures included."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.spec.write is None:
            raise exc.configuration(
                f"Cannot seed read-only spec '{self.spec.name}': seeding goes through the "
                "write path, so the spec needs a create command"
            )

        _require_countable(self.count, self.spec.name)

    # ....................... #

    @property
    def create_cmd(self) -> type[BaseModel]:
        """The command type every row of this spec is created through."""

        if self.spec.write is None:  # pragma: no cover - refused in __attrs_post_init__
            raise exc.configuration(f"Spec '{self.spec.name}' has no write side")

        # ``DocumentWriteTypes`` is a TypedDict, so this is a key, not an attribute.
        return self.spec.write["create_cmd"]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchSeed:
    """Documents to put in one search index.

    A separate type from :class:`SpecSeed` rather than a mode of it: a search index is
    filled by an ``upsert`` of the spec's own ``model_type``, not by a create command, and
    it has no create/update split, no ``rev`` and no link graph. Pretending the two are one
    shape would mean a seed type where half the fields are inert.
    """

    spec: SearchSpec[Any]
    """The search index to fill."""

    count: int = 0
    """How many documents to generate on top of :attr:`fixtures`."""

    fixtures: tuple[Mapping[str, Any], ...] = ()
    """Hand-written index documents, applied verbatim."""

    overrides: Mapping[str, Any] = attrs.field(factory=dict[str, Any])
    """Field values forced on every document."""

    ids_from: str | None = None
    """Take document ids from a seeded **document** spec instead of generating them.

    The referential-integrity story for this plane: an index whose ids name nothing is an
    index every result of which 404s when the client fetches the row behind it. Names a
    spec in :attr:`SeedPlan.specs`; generated rows beyond that pool keep their own ids.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_countable(self.count, str(self.spec.name))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StorageSeed:
    """Objects to upload into one storage spec."""

    spec: StorageSpec
    """The storage spec to fill."""

    objects: tuple[Mapping[str, Any], ...] = ()
    """Explicit uploads — ``{"filename": ..., "data": str | bytes, "prefix"?, "tags"?}``."""

    count: int = 0
    """How many filler objects to generate on top of :attr:`objects`."""

    prefix: str | None = None
    """Key prefix applied to generated objects (explicit ones may carry their own)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_countable(self.count, str(self.spec.name))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueSeed:
    """Messages to enqueue on one queue."""

    spec: QueueSpec[Any]
    """The queue spec whose codec types the payload."""

    channel: str
    """The queue name to enqueue on."""

    count: int = 0
    """How many payloads to generate on top of :attr:`fixtures`."""

    fixtures: tuple[Mapping[str, Any], ...] = ()
    """Hand-written payloads, applied verbatim."""

    overrides: Mapping[str, Any] = attrs.field(factory=dict[str, Any])
    """Field values forced on every payload."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _require_countable(self.count, f"{self.spec.name}/{self.channel}")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SeedPlan:
    """A deterministic description of a seeded world."""

    specs: tuple[SpecSeed, ...] = ()
    """The document specs to fill. Order is a tie-break only — dependencies decide the real
    order, and documents are always filled before the other planes so they can reference them."""

    search: tuple[SearchSeed, ...] = ()
    """Search indices to fill."""

    storage: tuple[StorageSeed, ...] = ()
    """Storage specs to fill."""

    queues: tuple[QueueSeed, ...] = ()
    """Queues to fill."""

    rng_seed: int = 0
    """Seeds generation and every link pick. The same plan and seed produce the same rows."""

    instant: datetime | None = DEFAULT_SEED_INSTANT
    """Clock the seed runs under (see :data:`DEFAULT_SEED_INSTANT`); ``None`` = wall clock."""

    links: Mapping[str, Mapping[str, str | None]] = attrs.field(
        factory=dict[str, Mapping[str, str | None]]
    )
    """Explicit ``{spec name: {field: target spec name}}``, overriding the inferred graph.

    The inference is name-driven and will miss (``owner_id`` does not look like ``users``);
    this is the correction. A ``None`` target opts a field **out** of linking — which is
    also how a reference cycle between two specs is broken.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [seed.spec.name for seed in self.specs]

        if len(set(names)) != len(names):
            duplicated = ", ".join(sorted({name for name in names if names.count(name) > 1}))

            raise exc.configuration(f"Spec seeded more than once: {duplicated}")

        unknown = sorted(set(self.links) - set(names))

        if unknown:
            raise exc.configuration(
                f"Link overrides name specs that are not seeded: {', '.join(unknown)}"
            )

        dangling = sorted(
            {seed.ids_from for seed in self.search if seed.ids_from and seed.ids_from not in names}
        )

        if dangling:
            raise exc.configuration(
                f"Search seeds take ids from specs that are not seeded: {', '.join(dangling)}"
            )

    # ....................... #

    def with_specs(self, *specs: SpecSeed) -> SeedPlan:
        """Return a plan with *specs* appended."""

        return attrs.evolve(self, specs=(*self.specs, *specs))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SeedResult:
    """What a seed created, per spec — the handles a test or a control plane needs."""

    ids: Mapping[str, tuple[UUID, ...]] = attrs.field(factory=dict[str, tuple[UUID, ...]])
    """Created document ids, in creation order, keyed by spec name."""

    rows: Mapping[str, tuple[JsonDict, ...]] = attrs.field(factory=dict[str, tuple[JsonDict, ...]])
    """The payloads as created, keyed by spec name — useful for asserting what was seeded."""

    indexed: Mapping[str, tuple[str, ...]] = attrs.field(factory=dict[str, tuple[str, ...]])
    """Search-document ids, keyed by index name."""

    stored: Mapping[str, tuple[str, ...]] = attrs.field(factory=dict[str, tuple[str, ...]])
    """Uploaded object keys, keyed by storage spec name."""

    queued: Mapping[str, int] = attrs.field(factory=dict[str, int])
    """How many messages were enqueued, keyed by ``"<spec>/<channel>"``."""

    # ....................... #

    def __getitem__(self, spec_name: str) -> tuple[UUID, ...]:
        return self.ids.get(spec_name, ())

    # ....................... #

    @property
    def total(self) -> int:
        """How many documents the seed created."""

        return sum(len(created) for created in self.ids.values())

    # ....................... #

    @property
    def total_all_planes(self) -> int:
        """Everything the seed wrote, across every plane."""

        return (
            self.total
            + sum(len(ids) for ids in self.indexed.values())
            + sum(len(keys) for keys in self.stored.values())
            + sum(self.queued.values())
        )


# ....................... #


def seed_plan(*specs: SpecSeed, rng_seed: int = 0) -> SeedPlan:
    """Convenience constructor for the common case: a few specs and a seed."""

    return SeedPlan(specs=specs, rng_seed=rng_seed)


# ....................... #


def spec_seed(
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    count: int = 0,
    fixtures: Sequence[Mapping[str, Any]] = (),
    overrides: Mapping[str, Any] | None = None,
) -> SpecSeed:
    """Convenience constructor accepting any sequence of fixtures."""

    return SpecSeed(
        spec=spec,
        count=count,
        fixtures=tuple(fixtures),
        overrides=dict(overrides or {}),
    )
