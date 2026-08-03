"""Apply a seed plan through the document write path."""

from __future__ import annotations

from contextlib import ExitStack
from random import Random
from typing import TYPE_CHECKING, Any, get_args
from uuid import UUID

from forze.base.primitives import (
    FrozenTimeSource,
    SeededEntropySource,
    bind_entropy_source,
    bind_time_source,
)

from .links import plan_links
from .plan import SeedPlan, SeedResult, SpecSeed
from .values import build_rows, split_row_id, validate_rows

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


def _accepts_none(create_cmd: type[Any], field: str) -> bool:
    """Whether the create command's *field* may be ``None``."""

    info = create_cmd.model_fields.get(field)

    return info is not None and type(None) in get_args(info.annotation)


# ....................... #


def _linked(
    row: dict[str, Any],
    *,
    seed: SpecSeed,
    targets: dict[str, str],
    created: dict[str, list[UUID]],
    rng: Random,
) -> dict[str, Any]:
    """Point this row's reference fields at documents the seed actually created.

    A self-reference draws from the rows already created for this spec, so a seeded tree
    gets real parents. The first row has nothing to point at: it becomes a root when the
    field is nullable, and only when it is not does the generated value stand — a dangling
    reference being strictly worse than none, since a client that follows it 404s.
    """

    for field, target in targets.items():
        pool = created.get(target, [])

        if pool:
            row[field] = pool[rng.randrange(len(pool))]

        elif _accepts_none(seed.create_cmd, field):
            row[field] = None

    return row


# ....................... #


async def apply_seed(ctx: ExecutionContext, plan: SeedPlan) -> SeedResult:
    """Create every document *plan* describes, and return what was created.

    Rows go through each spec's ``create_cmd`` and the document **command port** — never
    into ``MockState`` directly. That is the whole reason this is trustworthy seed data:
    ``rev``, timestamps, materialized computed fields and field encryption are produced by
    the same code path that serves the reads, so a seeded row is indistinguishable from one
    the app wrote.

    Determinism is end-to-end. Values come from ``plan.rng_seed``; ids and timestamps come
    from the write path, so unless the clock is pinned too they differ on every run — hence
    ``plan.instant``, which binds a frozen time source and a seeded entropy source for the
    duration of the seed. With it, two processes running the same plan produce byte-identical
    documents.
    """

    links, ordered = plan_links(plan)
    # The seam, not a bare `Random`: one seeded entropy source drives generation and every
    # link pick. A *second* source of the same seed is bound below for the write path, so
    # the two streams stay independent — changing how links are drawn must not renumber ids.
    rng = SeededEntropySource(seed=plan.rng_seed).as_random()
    created: dict[str, list[UUID]] = {}
    payloads: dict[str, tuple[dict[str, Any], ...]] = {}

    with ExitStack() as clock:
        if plan.instant is not None:
            clock.enter_context(bind_time_source(FrozenTimeSource(instant=plan.instant)))
            clock.enter_context(bind_entropy_source(SeededEntropySource(seed=plan.rng_seed)))

        for seed in ordered:
            name = seed.spec.name
            rows = build_rows(seed, rng)
            validate_rows(seed, rows)

            targets = dict(links.get(name, {}))
            command = ctx.doc.command(seed.spec)
            ids: list[UUID] = []
            created[name] = ids

            for row in rows:
                explicit_id, payload = split_row_id(row)
                linked = _linked(payload, seed=seed, targets=targets, created=created, rng=rng)
                document = await command.create(
                    seed.create_cmd(**linked),
                    id=explicit_id,
                    return_new=True,
                )
                ids.append(document.id)

            payloads[name] = tuple(rows)

    return SeedResult(
        ids={name: tuple(ids) for name, ids in created.items()},
        rows=payloads,
    )
