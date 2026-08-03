"""Deterministic, spec-driven seed data for the in-memory mock.

An empty store answers `GET /products` with `[]`, which no frontend and few tests can build
against. This fills it — through each spec's **write path**, so a seeded row carries the
same ``rev``, timestamps, materialized fields and field encryption a row the app wrote would
carry, and the read path returns it faithfully.

    from forze_mock.seeding import SeedPlan, apply_seed, spec_seed

    plan = SeedPlan(
        specs=(
            spec_seed(project_spec, count=3),
            spec_seed(task_spec, count=12, fixtures=load_fixtures("tasks.json")),
        ),
        rng_seed=7,
    )
    result = await apply_seed(ctx, plan)   # result["tasks"] -> the created ids

Three properties are the point:

* **Deterministic** — one ``rng_seed`` reproduces the values, and ``SeedPlan.instant`` pins
  the clock the write path mints ids and timestamps from, so the same plan produces
  byte-identical documents in another process.
* **Referentially whole** — a seeded ``Task.project_id`` names a seeded project, inferred
  from the names and corrected by ``SeedPlan.links``.
* **Plausible where it matters** — generated rows give volume, fixtures give a demo rows
  worth looking at.

Requires ``polyfactory`` (ships with the ``dst`` extra); importing this module without it
raises with that instruction.
"""

from forze_mock._compat import require_seeding

require_seeding()

# ....................... #

from .apply import apply_seed
from .links import infer_links, seed_order, singularize
from .plan import (
    DEFAULT_SEED_INSTANT,
    SeedPlan,
    SeedResult,
    SpecSeed,
    seed_plan,
    spec_seed,
)
from .values import ROW_ID, build_rows, load_fixtures

# ----------------------- #

__all__ = [
    "DEFAULT_SEED_INSTANT",
    "ROW_ID",
    "SeedPlan",
    "SeedResult",
    "SpecSeed",
    "apply_seed",
    "build_rows",
    "infer_links",
    "load_fixtures",
    "seed_order",
    "seed_plan",
    "singularize",
    "spec_seed",
]
