"""Referential integrity: which seeded spec a field points at, and what to fill first.

Same name-driven idea as ``forze_dst.derive`` (``order_id`` → the ``order`` pool), applied
to document specs rather than an operation catalog — and reimplemented rather than imported,
because ``forze_mock`` must not depend on a sibling integration package. The difference that
matters: spec names are plural (``projects``) and reference fields are singular
(``project_id``), so matching runs over a crude singular form of both.

The inference is a best guess, never an oracle. ``SeedPlan.links`` is the correction, and a
``None`` target there opts a field out entirely.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from forze.base.exceptions import exc

from .plan import SeedPlan, SpecSeed

# ----------------------- #

_ID_SUFFIXES = ("_id", "_uuid", "_key")

_NEVER_LINKED = frozenset({"id", "rev", "created_at", "last_update_at"})
"""Document identity/bookkeeping fields — never a reference to another spec."""

# ....................... #


def singularize(name: str) -> str:
    """A crude singular form, enough to match ``project_id`` against ``projects``."""

    for plural, singular in (("ies", "y"), ("ches", "ch"), ("shes", "sh"), ("sses", "ss")):
        if name.endswith(plural):
            return name[: -len(plural)] + singular

    if name.endswith("s") and not name.endswith("ss"):
        return name[:-1]

    return name


# ....................... #


def _referenced_stem(field: str) -> str | None:
    """The entity a field name references: ``project_id`` → ``project``."""

    if field in _NEVER_LINKED:
        return None

    for suffix in _ID_SUFFIXES:
        if field.endswith(suffix):
            return field[: -len(suffix)] or None

    return None


# ....................... #


def _singular_index(seeds: Sequence[SpecSeed]) -> dict[str, str]:
    """Map each seeded spec's singular stem back to its name, refusing a tie.

    Two specs sharing a stem (``boxes`` and ``box``) would otherwise have the later one
    silently win every reference field, and which one that is depends on plan order — the
    field would link to a spec the author never named.
    """

    index: dict[str, str] = {}

    for seed in seeds:
        stem = singularize(seed.spec.name)
        clashes = index.get(stem)

        if clashes is not None:
            raise exc.configuration(
                f"Specs '{clashes}' and '{seed.spec.name}' both reduce to '{stem}', so a "
                f"'{stem}_id' field cannot be linked unambiguously. Name the target "
                "explicitly with SeedPlan.links"
            )

        index[stem] = seed.spec.name

    return index


# ....................... #


def infer_links(
    seeds: Sequence[SpecSeed],
    *,
    overrides: Mapping[str, Mapping[str, str | None]] | None = None,
) -> Mapping[str, Mapping[str, str]]:
    """Map ``{spec name: {create-command field: target spec name}}``.

    Only fields whose *stem* matches a seeded spec are linked — a reference to something
    outside the plan cannot be satisfied and is left to generation.
    """

    by_singular = _singular_index(seeds)
    seeded = {seed.spec.name for seed in seeds}
    resolved: dict[str, dict[str, str]] = {}

    for seed in seeds:
        name = seed.spec.name
        forced = dict((overrides or {}).get(name, {}))
        fields: dict[str, str] = {}

        for field in seed.create_cmd.model_fields:
            if field in forced:
                target = forced.pop(field)

                if target is None:  # explicitly unlinked
                    continue

                if target not in seeded:
                    raise exc.configuration(
                        f"Link override '{name}.{field}' targets '{target}', which is not seeded"
                    )

                fields[field] = target
                continue

            stem = _referenced_stem(field)
            target_name = by_singular.get(singularize(stem)) if stem else None

            if target_name is not None:
                fields[field] = target_name

        if forced:
            unknown = ", ".join(sorted(forced))

            raise exc.configuration(
                f"Link overrides name fields '{name}' does not create: {unknown}"
            )

        if fields:
            resolved[name] = fields

    return resolved


# ....................... #


def seed_order(
    seeds: Sequence[SpecSeed],
    links: Mapping[str, Mapping[str, str]],
) -> tuple[SpecSeed, ...]:
    """Order the specs so a link target is always seeded before the spec referencing it.

    Self-references do not constrain the order — a row links to an earlier row of its own
    spec. A cycle *between* specs cannot be satisfied in any order and is refused; break it
    with a ``None`` link override on one side.
    """

    by_name = {seed.spec.name: seed for seed in seeds}
    pending = {
        name: {target for target in targets.values() if target != name}
        for name, targets in ((seed.spec.name, links.get(seed.spec.name, {})) for seed in seeds)
    }

    ordered: list[SpecSeed] = []
    placed: set[str] = set()

    while len(ordered) < len(seeds):
        # Plan order is the tie-break, so the result is stable for a given plan.
        ready = [name for name in by_name if name not in placed and pending[name] <= placed]

        if not ready:
            blocked = ", ".join(sorted(set(by_name) - placed))

            raise exc.configuration(
                f"Seeded specs reference each other in a cycle: {blocked}. "
                "Break it with a SeedPlan link override targeting None."
            )

        for name in ready:
            ordered.append(by_name[name])
            placed.add(name)

    return tuple(ordered)


# ....................... #


def plan_links(plan: SeedPlan) -> tuple[Mapping[str, Mapping[str, str]], tuple[SpecSeed, ...]]:
    """Resolve *plan*'s link graph and the order its specs must be filled in."""

    links = infer_links(plan.specs, overrides=plan.links)

    return links, seed_order(plan.specs, links)
