"""Row values: generated for volume, fixtures for plausibility."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from random import Random
from typing import Any
from uuid import UUID

import yaml

from forze.base.exceptions import exc

from .plan import SpecSeed

# ----------------------- #


def load_fixtures(path: str | Path) -> tuple[Mapping[str, Any], ...]:
    """Read a fixture file — JSON or YAML, a list of row mappings.

    Kept out of :class:`SpecSeed` on purpose: a registration value object that reads files
    when constructed cannot be built anywhere a plan is declared.
    """

    source = Path(path)

    if not source.is_file():
        raise exc.configuration(f"Fixture file not found: {source}")

    raw = source.read_text(encoding="utf-8")
    loaded = json.loads(raw) if source.suffix == ".json" else yaml.safe_load(raw)

    if not isinstance(loaded, list) or not all(isinstance(row, Mapping) for row in loaded):  # pyright: ignore[reportUnknownVariableType]
        raise exc.configuration(f"Fixture file {source} must hold a list of row mappings")

    return tuple(dict(row) for row in loaded)  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]


# ....................... #


def generated_rows(model: type[Any], count: int, rng: Random) -> tuple[dict[str, Any], ...]:
    """Generate *count* shape-correct rows for a pydantic *model*.

    Plane-agnostic on purpose: a document's create command, a search index's model and a
    queue payload are all just models to fill, and one generator keeps the determinism story
    identical across the planes.
    """

    if count == 0:
        return ()

    from polyfactory.factories.pydantic_factory import ModelFactory

    # A factory per call site, seeded from the plan's rng so the whole plan reproduces from
    # one number — and drawn in a fixed order, so adding a spec does not reshuffle the others.
    factory = ModelFactory.create_factory(model)
    factory.seed_random(rng.getrandbits(32))

    return tuple(factory.build().model_dump() for _ in range(count))


# ....................... #


def build_rows(seed: SpecSeed, rng: Random) -> tuple[dict[str, Any], ...]:
    """The payload mappings for one spec: fixtures first, then generated rows.

    Fixtures come first so a demo's hand-written rows are the ones a first page shows.
    :attr:`SpecSeed.overrides` is applied to every row, fixtures included — it is the
    per-spec constant (a tenant, an owner), not a default.
    """

    return merged_rows(
        fixtures=seed.fixtures,
        model=seed.create_cmd,
        count=seed.count,
        overrides=seed.overrides,
        rng=rng,
    )


# ....................... #


def merged_rows(
    *,
    fixtures: Sequence[Mapping[str, Any]],
    model: type[Any],
    count: int,
    overrides: Mapping[str, Any],
    rng: Random,
) -> tuple[dict[str, Any], ...]:
    """Fixtures first, then generated rows, with *overrides* forced on every one."""

    rows: list[dict[str, Any]] = [dict(row) for row in fixtures]
    rows.extend(generated_rows(model, count, rng))

    for row in rows:
        row.update(overrides)

    return tuple(rows)


# ....................... #


ROW_ID = "id"
"""Reserved fixture key: the document id to create the row under.

Not a create-command field — the write path mints ids — but a fixture sometimes has to name
one: an identity principal must exist under the id its credential maps to, or authentication
fails against a row that is otherwise correct.
"""


# ....................... #


def split_row_id(row: Mapping[str, Any]) -> tuple[UUID | None, dict[str, Any]]:
    """Separate a fixture's reserved id from the payload the create command receives."""

    payload = dict(row)
    raw = payload.pop(ROW_ID, None)

    if raw is None:
        return None, payload

    if isinstance(raw, UUID):
        return raw, payload

    try:
        return UUID(str(raw)), payload

    except ValueError as error:
        raise exc.configuration(f"Fixture 'id' is not a UUID: {raw!r}") from error


# ....................... #


def validate_rows(seed: SpecSeed, rows: Sequence[Mapping[str, Any]]) -> None:
    """Refuse fixture fields the create command does not accept, naming them.

    Pydantic would refuse them later with a per-row error; saying it once, up front, with
    the spec and the field names is the difference between a typo you fix and a stack trace
    you read.
    """

    known = set(seed.create_cmd.model_fields) | {ROW_ID}

    for index, row in enumerate(rows):
        unknown = sorted(set(row) - known)

        if unknown:
            raise exc.configuration(
                f"Fixture row {index} for '{seed.spec.name}' has fields the create command "
                f"does not accept: {', '.join(unknown)}"
            )
