"""The derived-read differential — does the mock's stand-in for a view answer what the view does?

A derived read field has two implementations by construction. The real backend reads the
view's column; the mock joins the source row itself (a resolved
:class:`~forze.application.contracts.conformity.DerivedReadField`) or asks a fixture callback
the test registered (a marked one). Nothing ties the two together, so a hand-written join that
drifted from the view keeps every mock-backed test green while production reads something else.

This leg measures the drift for one spec. The same seed is loaded into the real store and into
the mock, the same ids are read through both, and only the derived fields are compared — one
:class:`DerivedMismatch` per field per row, so the report names the declaration that is wrong
and the data it is wrong on, the way the isolation battery names the anomaly instead of failing
as a whole.

The seed is a callable rather than a plan, because this package does not depend on the mock:
pass ``lambda ctx: apply_seed(ctx, plan)`` with one :class:`~forze_mock.seeding.SeedPlan`. Keep
its instant pinned (the default) so both stores mint the same ids, and seed no derived values
(``SpecSeed.derived``): the real store has nowhere to put them, and on the mock they would
compare a fixture with the view rather than the stand-in the tests actually run on.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Protocol, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentQueryPort, DocumentSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, exc

# ----------------------- #


class SeededIds(Protocol):
    """The part of a seed result this leg reads — ``SeedResult`` satisfies it."""

    def __getitem__(self, spec_name: str) -> Sequence[UUID]: ...  # pragma: no cover


SeedFn = Callable[[ExecutionContext], Awaitable[SeededIds]]
"""Load one seed into the given context and return the ids it created, by spec name."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class ReadRefused:
    """A read that raised instead of returning the row, standing in for every value on it.

    Kept as a value rather than raised, so one refused row is a finding next to the others
    instead of the end of the run. Compared by kind only: messages are backend prose.
    """

    kind: str


# ....................... #


@final
@attrs.frozen(kw_only=True)
class DerivedMismatch:
    """One derived field on one row where the mock and the real backend disagree."""

    field: str
    id: UUID
    real: Any
    """The real backend's value, JSON-normalised, or a :class:`ReadRefused`."""

    mock: Any
    """The mock's value, JSON-normalised, or a :class:`ReadRefused`."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class DerivedFidelityReport:
    """Every derived field of one spec, compared on every seeded row."""

    spec: str
    fields: tuple[str, ...]
    ids: tuple[UUID, ...]
    mismatches: tuple[DerivedMismatch, ...]

    # ....................... #

    @property
    def agrees(self) -> bool:
        """Whether every derived field matched on every row."""

        return not self.mismatches

    # ....................... #

    def render_markdown(self) -> str:
        """A per-field summary, then one line per mismatching row."""

        lines = [
            f"# Derived-read fidelity: `{self.spec}` ({len(self.ids)} rows)",
            "",
            "| field | agree | diverge |",
            "|---|---|---|",
        ]

        for name in self.fields:
            diverge = sum(1 for mismatch in self.mismatches if mismatch.field == name)
            lines.append(f"| `{name}` | {len(self.ids) - diverge} | {diverge} |")

        if self.mismatches:
            lines += ["", "| field | id | real | mock |", "|---|---|---|---|"]
            lines += [
                f"| `{m.field}` | `{m.id}` | `{m.real!r}` | `{m.mock!r}` |" for m in self.mismatches
            ]

        return "\n".join(lines) + "\n"


# ....................... #


async def _derived_values(
    port: DocumentQueryPort[Any],
    pk: UUID,
    fields: tuple[str, ...],
) -> Mapping[str, Any]:
    try:
        row = await port.get(pk)

    except CoreException as error:
        return dict.fromkeys(fields, ReadRefused(kind=str(error.kind)))

    # JSON mode, so a nested model, a UUID and a datetime compare as the same value
    # whichever backend's decode produced them.
    dumped = row.model_dump(mode="json", include=set(fields))

    return {name: dumped.get(name) for name in fields}


# ....................... #


async def run_derived_fidelity(
    *,
    spec: DocumentSpec[Any, Any, Any, Any],
    seed: SeedFn,
    real: ExecutionContext,
    mock: ExecutionContext,
) -> DerivedFidelityReport:
    """Seed both stores with *seed*, read the same rows of *spec*, and diff its derived fields.

    *real* resolves *spec* against the backend whose relation produces the derived fields (a
    view); *mock* against the mock, with whatever resolves them there — a join declared on the
    spec, or a ``MockDerivedRegistry`` source on the module. Each store must start without the
    seeded rows.

    :raises exc.configuration: when *spec* declares no derived fields, the seed creates no row
        of it, or the two stores minted different ids, so their rows cannot be paired.
    """

    fields = tuple(sorted(spec.derived_read_fields))

    if not fields:
        raise exc.configuration(
            f"Spec {spec.name!r} declares no derived read fields; there is nothing to compare.",
        )

    name = str(spec.name)
    ids = tuple((await seed(real))[name])
    mock_ids = tuple((await seed(mock))[name])

    if not ids:
        raise exc.configuration(f"The seed creates no rows of spec {name!r} to compare.")

    if ids != mock_ids:
        raise exc.configuration(
            f"The real store and the mock minted different ids for one seed of {name!r}, so "
            "their rows cannot be paired. Keep the plan's instant pinned (the default), or "
            "give the fixtures explicit ids.",
        )

    real_port, mock_port = real.doc.query(spec), mock.doc.query(spec)
    mismatches: list[DerivedMismatch] = []

    for pk in ids:
        theirs = await _derived_values(real_port, pk, fields)
        ours = await _derived_values(mock_port, pk, fields)
        mismatches.extend(
            DerivedMismatch(field=field, id=pk, real=theirs[field], mock=ours[field])
            for field in fields
            if theirs[field] != ours[field]
        )

    return DerivedFidelityReport(spec=name, fields=fields, ids=ids, mismatches=tuple(mismatches))
