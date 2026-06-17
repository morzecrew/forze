"""Auto-derive a draft :class:`Scenario` from the operation catalog.

The turnkey-er half of the generative model: instead of hand-writing every :class:`Rule`,
read the registry's catalog and infer a *data-dependency graph* — which operations
**produce** entities and which **consume** them — then emit arrange (producer) and act
(consumer) rules that thread real ids through. The author refines the draft; they don't
start from a blank page.

The inference is **heuristic and name-driven** (the catalog carries input/output types, not
semantics):

* a *producer* is an op whose name starts with a creation verb (``create_order`` →
  produces the ``order`` entity); its real return is captured as the handle.
* a *consumer* is an op with an input field that references a known entity
  (``order_id`` / ``order`` → requires the ``order`` pool); that field is filled from the
  pool, other fields are auto-generated.

Type collisions and unconventional names will miss — :func:`derive_scenario` returns a best
guess, not an oracle. Treat its output as a starting `Scenario` to adjust, and override the
heuristics via the verb set or by editing the returned rules.
"""

from __future__ import annotations

import random
from typing import Any, Callable

from pydantic import BaseModel

from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze_dst.scenario import ModelState, Rule, Scenario

# ----------------------- #

DEFAULT_CREATE_VERBS = frozenset(
    {
        "create",
        "make",
        "new",
        "register",
        "open",
        "add",
        "start",
        "provision",
        "issue",
        "spawn",
        "place",
    }
)
"""Op-name prefixes that mark an operation as producing an entity."""

_ID_SUFFIXES = ("_id", "_uuid", "_key")


def _entity_produced_by(op: str, verbs: frozenset[str]) -> str | None:
    """The entity ``op`` produces by its name, e.g. ``create_order`` → ``order``."""

    parts = op.replace("-", "_").split("_")

    return "_".join(parts[1:]) if len(parts) >= 2 and parts[0] in verbs else None


def _entity_for_field(field: str, entities: frozenset[str]) -> str | None:
    """The known entity a field name references, e.g. ``order_id`` → ``order``."""

    if field in entities:
        return field

    for suffix in _ID_SUFFIXES:
        if field.endswith(suffix):
            stem = field[: -len(suffix)]
            if stem in entities:
                return stem

    return None


def _arg_builder(
    input_type: type[BaseModel] | None,
    entity_fields: dict[str, str],
) -> Callable[[ModelState, random.Random], Any]:
    """Build an input from the model: entity fields ← arranged handles, rest auto-generated."""

    if input_type is None:
        return lambda _state, _rng: None

    def build(state: ModelState, rng: random.Random) -> Any:
        picks = {
            field: state.pick(entity, rng) for field, entity in entity_fields.items()
        }
        non_entity = set(input_type.model_fields) - set(entity_fields)

        if not non_entity:  # every field is an arranged handle — no generator needed
            return input_type(**picks)

        from polyfactory.factories.pydantic_factory import ModelFactory

        factory = ModelFactory.create_factory(input_type)
        factory.seed_random(rng.getrandbits(32))
        generated = factory.build().model_dump()
        generated.update(picks)
        return input_type.model_validate(generated)

    return build


# ....................... #


def derive_scenario(
    registry: FrozenOperationRegistry,
    *,
    create_verbs: frozenset[str] = DEFAULT_CREATE_VERBS,
    arrange_each: int = 1,
) -> Scenario:
    """Infer a draft :class:`Scenario` from *registry*'s catalog (best-effort, name-driven).

    Producers (creation-verb names) become arrange rules — fired *arrange_each* times each —
    producing an entity pool; every other op with an input field referencing a known entity
    becomes an act rule that requires that pool and fills the field from it. Ops with no
    entity reference become unconstrained act rules (inputs auto-generated).
    """

    catalog = registry.catalog()

    producers = {
        str(op): entity
        for op, _ in catalog.items()
        if (entity := _entity_produced_by(str(op), create_verbs)) is not None
    }
    entities = frozenset(producers.values())

    arrange = tuple(
        Rule(op=op, produces=entity)
        for op, entity in producers.items()
        for _ in range(arrange_each)
    )

    act: list[Rule] = []

    for op, entry in catalog.items():
        op = str(op)

        if op in producers:  # producers are arrange-only by default
            continue

        input_type = entry.descriptor.input_type if entry.descriptor else None
        entity_fields = (
            {
                field: entity
                for field in input_type.model_fields
                if (entity := _entity_for_field(field, entities)) is not None
            }
            if input_type is not None
            else {}
        )

        act.append(
            Rule(
                op=op,
                requires=tuple(sorted(set(entity_fields.values()))),
                arg=_arg_builder(input_type, entity_fields),
            )
        )

    return Scenario(state=ModelState, arrange=arrange, act=tuple(act))
