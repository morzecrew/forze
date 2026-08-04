"""Apply a seed plan through the document write path."""

from __future__ import annotations

from contextlib import ExitStack
from random import Random
from typing import TYPE_CHECKING, Any, get_args
from uuid import UUID

from forze.application.contracts.queue import QueueCommandDepKey
from forze.application.contracts.storage import UploadedObject
from forze.base.exceptions import exc
from forze.base.primitives import (
    FrozenTimeSource,
    SeededEntropySource,
    bind_entropy_source,
    bind_time_source,
)

from .links import plan_links
from .plan import SeedPlan, SeedResult, SpecSeed
from .values import build_rows, merged_rows, split_row_id, validate_rows

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
    field is nullable, and the seed is refused when it is not — a dangling reference is
    strictly worse than none, since a client that follows it 404s, and letting the generated
    value stand would ship exactly that. The way out is an explicit ``links`` entry: naming
    a target that gets seeded, or ``None`` to opt the field out of linking entirely.
    """

    for field, target in targets.items():
        pool = created.get(target, [])

        if pool:
            row[field] = pool[rng.randrange(len(pool))]

        elif _accepts_none(seed.create_cmd, field):
            row[field] = None

        else:
            raise exc.configuration(
                f"Cannot link '{seed.spec.name}.{field}' to '{target}': no rows to point at "
                f"({'the first row of a self-reference' if target == seed.spec.name else 'that spec seeds nothing'}), "
                f"and the field is not nullable. Seed '{target}' first, make the field "
                f"optional, or opt out with links={{'{seed.spec.name}': {{'{field}': None}}}}"
            )

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
            written: list[dict[str, Any]] = []

            for row in rows:
                explicit_id, payload = split_row_id(row)
                linked = _linked(payload, seed=seed, targets=targets, created=created, rng=rng)
                created_cmd = seed.create_cmd(**linked)
                document = await command.create(created_cmd, id=explicit_id, return_new=True)
                ids.append(document.id)
                # The *validated command*, not the row it came from: the row still carries the
                # reserved `id` key the create command never sees, and its reference fields are
                # the generated ones, from before linking resolved them.
                written.append(created_cmd.model_dump(mode="json"))

            payloads[name] = tuple(written)

        indexed = await _apply_search(ctx, plan, created, rng)
        stored = await _apply_storage(ctx, plan)
        queued = await _apply_queues(ctx, plan, rng)

    return SeedResult(
        ids={name: tuple(ids) for name, ids in created.items()},
        rows=payloads,
        indexed=indexed,
        stored=stored,
        queued=queued,
    )


# ....................... #


async def _apply_search(
    ctx: ExecutionContext,
    plan: SeedPlan,
    created: dict[str, list[UUID]],
    rng: Random,
) -> dict[str, tuple[str, ...]]:
    """Fill each search index through its **upsert** — the plane's own write path.

    Ids come from a seeded document spec when ``ids_from`` says so: an index whose ids name
    nothing is an index every hit of which 404s the moment the client fetches the row.
    """

    indexed: dict[str, tuple[str, ...]] = {}

    for seed in plan.search:
        model = seed.spec.model_type
        rows = merged_rows(
            fixtures=seed.fixtures,
            model=model,
            count=seed.count,
            overrides=seed.overrides,
            rng=rng,
        )

        if seed.ids_from is not None:
            pool = created.get(seed.ids_from, [])
            rows = tuple(
                {**row, "id": str(pool[index])} if index < len(pool) else row
                for index, row in enumerate(rows)
            )

        documents = [model(**row) for row in rows]

        if documents:
            await ctx.search.command(seed.spec).upsert(documents)

        indexed[str(seed.spec.name)] = tuple(str(document.id) for document in documents)

    return indexed


# ....................... #


async def _apply_storage(
    ctx: ExecutionContext,
    plan: SeedPlan,
) -> dict[str, tuple[str, ...]]:
    """Upload each object through the storage command port."""

    stored: dict[str, tuple[str, ...]] = {}

    for seed in plan.storage:
        uploads = [
            UploadedObject(
                filename=str(obj["filename"]),
                data=_as_bytes(obj.get("data", b"")),
                prefix=obj.get("prefix", seed.prefix),
                tags=obj.get("tags"),
            )
            for obj in seed.objects
        ]

        # Filler blobs are deterministic bytes, not random ones: a seed that changes its
        # payloads between runs is a seed you cannot diff a response against.
        uploads.extend(
            UploadedObject(
                filename=f"seeded-{index}.txt",
                data=f"seeded object {index}".encode(),
                prefix=seed.prefix,
            )
            for index in range(seed.count)
        )

        command = ctx.storage.command(seed.spec)
        keys: list[str] = []

        for upload in uploads:
            result = await command.upload(upload)
            keys.append(str(result.key))

        stored[str(seed.spec.name)] = tuple(keys)

    return stored


# ....................... #


async def _apply_queues(
    ctx: ExecutionContext,
    plan: SeedPlan,
    rng: Random,
) -> dict[str, int]:
    """Enqueue each payload through the queue command port."""

    queued: dict[str, int] = {}

    for seed in plan.queues:
        model = seed.spec.codec.model_type
        rows = merged_rows(
            fixtures=seed.fixtures,
            model=model,
            count=seed.count,
            overrides=seed.overrides,
            rng=rng,
        )

        # No `ctx.queue` accessor exists; this is the resolution the outbox relay uses,
        # route and all, so a routed real queue resolves the same way it does in production.
        command = ctx.deps.resolve_configurable(
            ctx,
            QueueCommandDepKey,
            seed.spec,
            route=seed.spec.name,
        )

        for row in rows:
            await command.enqueue(seed.channel, model(**row))

        queued[f"{seed.spec.name}/{seed.channel}"] = len(rows)

    return queued


# ....................... #


def _as_bytes(data: Any) -> bytes:
    """Accept a fixture's ``data`` as text or bytes."""

    return data if isinstance(data, bytes) else str(data).encode()
