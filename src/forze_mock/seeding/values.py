"""Row values: generated for volume, fixtures for plausibility."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from random import Random
from typing import Any

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


def _generated_rows(seed: SpecSeed, rng: Random) -> tuple[dict[str, Any], ...]:
    """Generate *count* shape-correct rows for the spec's create command."""

    if seed.count == 0:
        return ()

    from polyfactory.factories.pydantic_factory import ModelFactory

    # A factory per spec, seeded from the plan's rng so the whole plan reproduces from one
    # number — and drawn in a fixed order, so adding a spec does not reshuffle the others.
    factory = ModelFactory.create_factory(seed.create_cmd)
    factory.seed_random(rng.getrandbits(32))

    return tuple(factory.build().model_dump() for _ in range(seed.count))


# ....................... #


def build_rows(seed: SpecSeed, rng: Random) -> tuple[dict[str, Any], ...]:
    """The payload mappings for one spec: fixtures first, then generated rows.

    Fixtures come first so a demo's hand-written rows are the ones a first page shows.
    :attr:`SpecSeed.overrides` is applied to every row, fixtures included — it is the
    per-spec constant (a tenant, an owner), not a default.
    """

    rows: list[dict[str, Any]] = [dict(row) for row in seed.fixtures]
    rows.extend(_generated_rows(seed, rng))

    for row in rows:
        row.update(seed.overrides)

    return tuple(rows)


# ....................... #


def validate_rows(seed: SpecSeed, rows: Sequence[Mapping[str, Any]]) -> None:
    """Refuse fixture fields the create command does not accept, naming them.

    Pydantic would refuse them later with a per-row error; saying it once, up front, with
    the spec and the field names is the difference between a typo you fix and a stack trace
    you read.
    """

    known = set(seed.create_cmd.model_fields)

    for index, row in enumerate(rows):
        unknown = sorted(set(row) - known)

        if unknown:
            raise exc.configuration(
                f"Fixture row {index} for '{seed.spec.name}' has fields the create command "
                f"does not accept: {', '.join(unknown)}"
            )
