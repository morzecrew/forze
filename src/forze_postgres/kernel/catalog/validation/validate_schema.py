"""Compare Pydantic document shapes to Postgres relation columns (startup validation)."""

import re
from collections.abc import Sequence
from typing import Final

import attrs
from pydantic import BaseModel

from forze.application.contracts.guarantees import (
    NonOverlapping,
    StorageGuarantees,
    UniqueTogether,
)
from forze.application.contracts.querying import collect_filter_field_roots
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import exc
from forze.base.serialization import stored_field_names_for
from forze.domain.models import DocumentHistory
from forze_postgres.kernel._logger import logger
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector, PostgresType
from forze_postgres.kernel.gateways.types import PostgresBookkeepingStrategy
from forze_postgres.kernel.sql.conflict_target import resolve_write_conflict_target

from .validate_schema_types import (
    validate_field_nullability,
    validate_field_type_compatibility,
)

# ----------------------- #


def _write_field_names_union(
    domain: type[BaseModel],
    create: type[BaseModel],
    update: type[BaseModel] | None,
) -> frozenset[str]:
    names = stored_field_names_for(domain, include_computed=False) | stored_field_names_for(
        create,
        include_computed=False,
    )

    if update is not None:
        names |= stored_field_names_for(update, include_computed=False)

    return frozenset(names)


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDocumentSchemaSpec:
    """Describe one document's read/write (and optional history) relations for schema checks."""

    name: str
    """Document route name (for error messages)."""

    read_model: type[BaseModel]
    """Read model deserialized from the read relation."""

    read_relation: tuple[str, str]
    """Read relation ``(schema, name)``."""

    read_omit_fields: frozenset[str] = frozenset()
    """Read model field names that are not stored on the read relation (computed, etc.)."""

    tenant_aware: bool = False
    """When ``True``, the write relation must expose :data:`~forze.application.contracts.tenancy.TENANT_ID_FIELD`."""

    write_domain_model: type[BaseModel] | None = None
    """Domain document model (persisted row shape)."""

    write_create_model: type[BaseModel] | None = None
    """Create command model."""

    write_update_model: type[BaseModel] | None = None
    """Update command model, if any."""

    write_relation: tuple[str, str] | None = None
    """Write table ``(schema, name)``."""

    write_omit_fields: frozenset[str] = frozenset()
    """Write-side field names not mapped to columns (rare escape hatch)."""

    history_enabled: bool = False
    """When ``True``, validate the history relation if :attr:`history_relation` is set."""

    history_relation: tuple[str, str] | None = None
    """History table ``(schema, name)``."""

    history_omit_fields: frozenset[str] = frozenset()
    """History row field names omitted from the physical table."""

    materialized: frozenset[str] = frozenset()
    """``@computed_field`` names persisted as real columns on the read/write relations
    (see :attr:`DocumentSpec.materialized`); required to exist so a missing column
    fails at startup rather than on the first write."""

    bookkeeping_strategy: PostgresBookkeepingStrategy | None = None
    """Bookkeeping strategy for the write relation; ``None`` when read-only."""

    conflict_target: tuple[str, ...] | None = None
    """Optional ``ON CONFLICT`` columns for ensure/upsert; ``None`` infers PRIMARY KEY."""

    guarantees: StorageGuarantees = ()
    """What the spec requires the store to enforce (see :attr:`DocumentSpec.guarantees`).

    Validated against the live catalog: reconciliation at wiring says Postgres *can* keep
    these, and this says whether the deployment's migration actually did."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.history_enabled and self.history_relation is None:
            raise exc.internal(
                f"Document {self.name!r}: history_enabled requires history_relation.",
            )

        if self.write_relation is not None and (
            self.write_domain_model is None or self.write_create_model is None
        ):
            raise exc.internal(
                f"Document {self.name!r}: write_relation requires "
                "write_domain_model and write_create_model.",
            )


# ....................... #


async def _require_columns(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    required: frozenset[str],
    label: str,
) -> None:
    types = await introspector.get_column_types(schema=schema, relation=relation)

    if missing := required - frozenset(types.keys()):
        raise exc.internal(
            f"Postgres schema validation failed for {label!r} ({schema}.{relation}): "
            f"missing columns {sorted(missing)}.",
            code="postgres_schema_validation_failed",
            details={
                "document": label,
                "schema": schema,
                "relation": relation,
                "missing": sorted(missing),
            },
        )


# ....................... #


async def _validate_relation_models(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    models: Sequence[tuple[type[BaseModel], frozenset[str], str]],
) -> None:
    column_types = await introspector.get_column_types(schema=schema, relation=relation)

    for model, omit_fields, label in models:
        validate_field_type_compatibility(
            model=model,
            column_types=column_types,
            omit_fields=omit_fields,
            label=label,
        )
        validate_field_nullability(
            model=model,
            column_types=column_types,
            omit_fields=omit_fields,
            label=label,
        )


# ....................... #


async def _validate_tenant_column(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    label: str,
) -> None:
    types = await introspector.get_column_types(schema=schema, relation=relation)
    pg_t = types.get(TENANT_ID_FIELD)

    if pg_t is None:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r}: "
            f"tenant-aware document requires column {TENANT_ID_FIELD!r}.",
            code="postgres_schema_validation_failed",
            details={"label": label, "schema": schema, "relation": relation},
        )

    if pg_t.base != "uuid" or pg_t.is_array:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r}: "
            f"{TENANT_ID_FIELD!r} must be type uuid.",
            code="postgres_schema_validation_failed",
            details={
                "label": label,
                "actual_base": pg_t.base,
                "is_array": pg_t.is_array,
            },
        )

    if not pg_t.not_null:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r}: "
            f"{TENANT_ID_FIELD!r} must be NOT NULL.",
            code="postgres_schema_validation_failed",
            details={"label": label},
        )


# ....................... #


def _warn_read_not_subset_of_write(
    *,
    spec: PostgresDocumentSchemaSpec,
    write_column_types: dict[str, PostgresType],
) -> None:
    if spec.write_relation is None:
        return

    if spec.read_relation == spec.write_relation:
        return

    read_fields = (
        stored_field_names_for(spec.read_model, include_computed=False) | spec.materialized
    ) - spec.read_omit_fields

    if extra := read_fields - frozenset(write_column_types.keys()):
        logger.warning(
            "Postgres schema validation for document %r: read fields %s are not on "
            "write relation %s.%s (expected for views; silence with read_omit_fields).",
            spec.name,
            sorted(extra),
            spec.write_relation[0],
            spec.write_relation[1],
        )


# ....................... #


def _warn_unused_tenant_column(
    *,
    spec: PostgresDocumentSchemaSpec,
    write_column_types: dict[str, PostgresType],
) -> None:
    if spec.tenant_aware or spec.write_relation is None:
        return

    if TENANT_ID_FIELD not in write_column_types:
        return

    schema, relation = spec.write_relation
    logger.warning(
        "Postgres schema validation for document %r: write relation %s.%s has "
        "column %r but tenant_aware=False — row-level isolation is disabled; "
        "confirm wiring or enable tenant_aware.",
        spec.name,
        schema,
        relation,
        TENANT_ID_FIELD,
    )


# ....................... #


async def _require_guarantee_mechanisms(
    introspector: PostgresIntrospector,
    spec: PostgresDocumentSchemaSpec,
) -> None:
    """Refuse a declared guarantee whose index is not in the database, naming the DDL.

    Reconciliation at wiring proved Postgres *can* keep these; this is the other half — whether
    this deployment's migration did. Never creates anything: an adapter that issued the DDL
    would take a lock on a production table nobody asked for, and would hide the missing
    migration until the next deployment.

    Three ways an index can carry the right columns and still not be the mechanism:

    * it is not partial where the guarantee is filtered, or its predicate does not mention the
      fields the filter selects on — the case that matters, because a predicate over the wrong
      column leaves exactly the rows the guarantee covers unconstrained;
    * it is an ordinary unique index over a nullable column while the guarantee counts nulls as
      values, which Postgres does not unless the index says ``NULLS NOT DISTINCT``;
    * it is not valid, ready or live — the state a failed concurrent build leaves.

    What is still not checked is whether the predicate *means* the same as the filter: deciding
    that two boolean expressions agree is the database's job, not a startup check's. So the
    column-level comparison is a floor, not a proof, and the docs say so.
    """

    relation = spec.write_relation or spec.read_relation
    schema, table = relation

    guarantees = [g for g in spec.guarantees if isinstance(g, UniqueTogether)]

    if not guarantees:
        # Reconciliation refuses every other member today, so nothing else reaches here.
        return

    indexes = await introspector.unique_indexes(schema=schema, relation=table)
    column_types = await introspector.get_column_types(schema=schema, relation=table)

    for guarantee in guarantees:
        columns = tuple(guarantee.fields)
        wanted = frozenset(columns)
        # A filter over a column the index does not mention cannot restrict the guarantee's
        # rows, so the predicate has to name every field the declaration selects on — and, when
        # nulls are exempt, every field of the tuple, since that exemption *is* a predicate.
        predicate_columns = (
            collect_filter_field_roots(guarantee.where) if guarantee.where else frozenset()
        )

        if guarantee.skip_null:
            predicate_columns |= wanted

        nullable = sorted(
            column
            for column in columns
            if column in column_types and not column_types[column].not_null
        )
        # Only a nullable column can produce the divergence: Postgres compares two nulls as
        # distinct, so an ordinary unique index admits a pair of rows the guarantee refuses.
        # Over NOT NULL columns the ordinary index is exactly the mechanism, and demanding
        # NULLS NOT DISTINCT there would refuse a correct migration.
        needs_nulls_not_distinct = bool(nullable) and not guarantee.skip_null

        for index in indexes:
            if index.columns != wanted:
                continue

            if predicate_columns:
                if index.predicate is None:
                    continue

                if any(
                    not _predicate_names(index.predicate, column) for column in predicate_columns
                ):
                    continue

            elif index.predicate is not None:
                continue

            if needs_nulls_not_distinct and not index.nulls_not_distinct:
                continue

            break

        else:
            raise exc.configuration(
                _guarantee_refusal(
                    spec_name=str(spec.name),
                    schema=schema,
                    table=table,
                    columns=columns,
                    predicate_columns=predicate_columns,
                    nulls_not_distinct=needs_nulls_not_distinct,
                    nullable=nullable,
                ),
                details={
                    "document": spec.name,
                    "relation": f"{schema}.{table}",
                    "columns": list(columns),
                },
            )


# ....................... #


_LITERAL = re.compile(r"'(?:''|[^'])*'")
"""A single-quoted SQL string literal, doubled quotes included — stripped before identifiers are
matched, so a predicate comparing against ``'deleted'`` does not read as naming that column."""


def _predicate_names(predicate: str, column: str) -> bool:
    """Whether *predicate* references *column* as an identifier rather than as a substring.

    ``pg_get_expr`` hands back deparsed SQL and the only question asked of it is which columns it
    restricts on, so the match is on identifier boundaries after string literals are removed:
    ``deleted`` must not be satisfied by ``(deleted_at IS NULL)``, and must not be satisfied by
    ``(label <> 'deleted')`` either.

    Deliberately not a SQL lexer. What this decides is whether the predicate mentions the right
    column at all — a floor under an index restricted to the wrong rows, never a proof that it
    restricts to the right ones, since that is expression equivalence and the database's job. A
    lexer would buy exactness on quoted identifiers with unusual casing for a check that is
    approximate by construction.
    """

    bare = _LITERAL.sub("''", predicate)

    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(column)}(?![A-Za-z0-9_])", bare) is not None


# ....................... #

_RANGE_CALL = re.compile(r"\b\w*range\s*\((('[^']*'|[^()'])*)\)")
"""A range constructor call and its arguments — ``daterange(a, b, '[]')`` and its siblings.

Quoted strings are consumed whole, because three of the four bounds literals contain a
parenthesis and a naive scan ends the argument list inside ``'[)'``. Nested *calls* end the
match on purpose: a constructor whose arguments are themselves expressions is one this refuses
to interpret rather than half-parse."""

_BOUNDS_LITERAL = re.compile(r"'(\[\)|\[\]|\(\]|\(\))'")
"""The four bounds conventions, as Postgres spells them in a range constructor's third argument."""

BTREE_GIST: Final[str] = "btree_gist"
"""The extension that lets an exclusion constraint compare a scalar key with ``=``.

Named here and nowhere near the contract: a guarantee declares a property, and which extension
makes the mechanism available is this backend's business."""


def _range_bounds(definition: str) -> frozenset[str]:
    """Every bounds convention the range constructors in *definition* ask for.

    A two-argument constructor contributes ``"[)"``, which is Postgres's documented default and
    the reason a correct migration need not spell it. A constructor whose third argument is not
    one of the four literals contributes nothing, so it cannot satisfy any declaration — the
    fail-closed half, since an expression this cannot read is one whose convention is unknown.
    """

    found: set[str] = set()

    for args, _ in _RANGE_CALL.findall(definition):
        parts = [part.strip() for part in args.split(",")]

        if len(parts) >= 3:
            match = _BOUNDS_LITERAL.search(parts[2])

            if match is not None:
                found.add(match.group(1))

        elif len(parts) == 2:
            found.add("[)")

    return frozenset(found)


# ....................... #


async def _require_overlap_mechanisms(
    introspector: PostgresIntrospector,
    spec: PostgresDocumentSchemaSpec,
) -> None:
    """Refuse a declared non-overlap guarantee with no exclusion constraint behind it.

    The sibling of :func:`_require_guarantee_mechanisms` for the other member, and the same
    doctrine: reconciliation proved Postgres *can* keep this, and this asks whether the
    deployment's migration did. Nothing is created.

    Four ways a constraint can exist and still not be the mechanism:

    * it does not cover every key field, so rows the guarantee separates are compared — or not
      compared — by something other than the declaration;
    * its range expression does not mention both period fields;
    * it carries a **different bounds convention** than the declaration. The two conventions
      differ on exactly one day, which is the day a boundary bug lives on, so a ``'[]'``
      declaration validated against a ``'[)'`` constraint is the failure this check exists for.

    Deliberately no validity check, unlike the uniqueness pass: Postgres refuses ``NOT VALID``
    on an EXCLUDE constraint and has no concurrent build for one, so the half-built state that
    check guards against cannot arise here.
    """

    guarantees = [g for g in spec.guarantees if isinstance(g, NonOverlapping)]

    if not guarantees:
        return

    relation = spec.write_relation or spec.read_relation
    schema, table = relation

    constraints = await introspector.exclusion_constraints(schema=schema, relation=table)
    column_types = await introspector.get_column_types(schema=schema, relation=table)

    for guarantee in guarantees:
        key = frozenset(guarantee.key)
        start, end = guarantee.period

        for constraint in constraints:
            if not key <= constraint.columns:
                continue

            if not all(_predicate_names(constraint.definition, field) for field in (start, end)):
                continue

            if guarantee.bounds not in _range_bounds(constraint.definition):
                continue

            break

        else:
            raise exc.configuration(
                _overlap_refusal(
                    spec_name=str(spec.name),
                    schema=schema,
                    table=table,
                    guarantee=guarantee,
                    range_fn=_range_function(column_types.get(start)),
                    extension_missing=not await introspector.extension_installed(name=BTREE_GIST),
                ),
                details={
                    "document": spec.name,
                    "relation": f"{schema}.{table}",
                    "key": list(guarantee.key),
                    "period": list(guarantee.period),
                },
            )


# ....................... #


def _range_function(start: PostgresType | None) -> str:
    """The range constructor matching the period's start column.

    Only the printed DDL depends on this — the check itself reads columns and bounds, so a type
    this does not know still validates against whatever constructor the migration used.
    """

    base = (start.base if start is not None else "").lower()

    if base.startswith("timestamp"):
        return "tstzrange" if "with time zone" in base or base.endswith("tz") else "tsrange"

    return "daterange"


# ....................... #


def _overlap_refusal(
    *,
    spec_name: str,
    schema: str,
    table: str,
    guarantee: NonOverlapping,
    range_fn: str,
    extension_missing: bool,
) -> str:
    """The message for a non-overlap guarantee with no constraint behind it, carrying the DDL."""

    key_list = ", ".join(guarantee.key)
    start, end = guarantee.period
    key_elements = ", ".join(f"{field} WITH =" for field in guarantee.key)

    ddl = (
        f"ALTER TABLE {schema}.{table} ADD EXCLUDE USING gist ("
        f"{key_elements}, {range_fn}({start}, {end}, '{guarantee.bounds}') WITH &&);"
    )

    extension = (
        f"  CREATE EXTENSION IF NOT EXISTS {BTREE_GIST};  -- not installed; "
        f"gist cannot compare {key_list} with = without it\n"
        if extension_missing
        else ""
    )

    return (
        f"Document {spec_name!r} guarantees no two rows per ({key_list}) hold overlapping "
        f"periods over ({start}, {end}) with bounds {guarantee.bounds!r}, and {schema}.{table} "
        "has no EXCLUDE constraint that keeps it. A constraint carrying a different "
        "bounds convention does not count: the two disagree on exactly one boundary value, "
        "which is the one a reader would trust it for. The migration is what satisfies a "
        f"guarantee — nothing here creates one. This would:\n{extension}  {ddl}"
    )


# ....................... #


def _guarantee_refusal(
    *,
    spec_name: str,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    predicate_columns: frozenset[str],
    nulls_not_distinct: bool,
    nullable: Sequence[str],
) -> str:
    """The message for a guarantee with no index behind it, carrying the DDL that would serve.

    Split out because the refusal is most of the value here: an operator reading it has to be
    able to write the migration without opening the code, and which index is missing depends on
    three axes that the caller has already worked out.
    """

    column_list = ", ".join(columns)
    filtered = bool(predicate_columns)

    if filtered:
        ddl = (
            f"CREATE UNIQUE INDEX CONCURRENTLY ON {schema}.{table} ({column_list}) "
            f"WHERE <a condition over {', '.join(sorted(predicate_columns))}>;"
        )

    elif nulls_not_distinct:
        ddl = f"ALTER TABLE {schema}.{table} ADD UNIQUE NULLS NOT DISTINCT ({column_list});"

    else:
        ddl = f"ALTER TABLE {schema}.{table} ADD UNIQUE ({column_list});"

    why = " among the rows its filter selects, and " if filtered else ", and "
    missing = (
        f"{schema}.{table} has no valid partial unique index on those columns whose predicate "
        f"mentions {', '.join(sorted(predicate_columns))}"
        if filtered
        else f"{schema}.{table} has no valid unique index on those columns"
    )
    nulls = (
        f" declared NULLS NOT DISTINCT — {', '.join(nullable)} is nullable, and Postgres lets "
        "two rows share a tuple containing a null unless the index says otherwise, while the "
        "guarantee does not"
        if nulls_not_distinct
        else ""
    )

    return (
        f"Document {spec_name!r} guarantees at most one row per ({column_list})"
        + why
        + missing
        + nulls
        + ". The migration is what satisfies a guarantee — nothing here creates one. "
        f"This would:\n  {ddl}"
    )


# ....................... #


async def validate_postgres_document_schemas(
    introspector: PostgresIntrospector,
    specs: Sequence[PostgresDocumentSchemaSpec],
) -> None:
    """Assert each spec's relations expose the columns implied by the Pydantic models."""

    for spec in specs:
        await _require_guarantee_mechanisms(introspector, spec)
        await _require_overlap_mechanisms(introspector, spec)

        read_need = frozenset(
            (stored_field_names_for(spec.read_model, include_computed=False) | spec.materialized)
            - spec.read_omit_fields,
        )
        await _require_columns(
            introspector,
            schema=spec.read_relation[0],
            relation=spec.read_relation[1],
            required=read_need,
            label=f"{spec.name} read",
        )

        await _validate_relation_models(
            introspector,
            schema=spec.read_relation[0],
            relation=spec.read_relation[1],
            models=[
                (
                    spec.read_model,
                    spec.read_omit_fields,
                    f"{spec.name} read",
                ),
            ],
        )

        if spec.write_relation is not None:
            if spec.write_domain_model is None or spec.write_create_model is None:
                raise exc.internal(
                    f"Document {spec.name!r}: write_relation requires "
                    "write_domain_model and write_create_model.",
                )

            write_need = (
                _write_field_names_union(
                    spec.write_domain_model,
                    spec.write_create_model,
                    spec.write_update_model,
                )
                | spec.materialized
            ) - spec.write_omit_fields

            if spec.tenant_aware:
                write_need = write_need | {TENANT_ID_FIELD}

            await _require_columns(
                introspector,
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
                required=write_need,
                label=f"{spec.name} write",
            )

            write_models: list[tuple[type[BaseModel], frozenset[str], str]] = [
                (
                    spec.write_domain_model,
                    spec.write_omit_fields,
                    f"{spec.name} write domain",
                ),
                (
                    spec.write_create_model,
                    spec.write_omit_fields,
                    f"{spec.name} write create",
                ),
            ]

            if spec.write_update_model is not None:
                write_models.append(
                    (
                        spec.write_update_model,
                        spec.write_omit_fields,
                        f"{spec.name} write update",
                    ),
                )

            await _validate_relation_models(
                introspector,
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
                models=write_models,
            )

            if spec.tenant_aware:
                await _validate_tenant_column(
                    introspector,
                    schema=spec.write_relation[0],
                    relation=spec.write_relation[1],
                    label=f"{spec.name} write",
                )

            write_types = await introspector.get_column_types(
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
            )
            _warn_read_not_subset_of_write(spec=spec, write_column_types=write_types)
            _warn_unused_tenant_column(spec=spec, write_column_types=write_types)

            await resolve_write_conflict_target(
                introspector,
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
                configured=spec.conflict_target,
                document_label=spec.name,
            )

        if spec.history_relation is not None:
            hist_need = (
                frozenset(stored_field_names_for(DocumentHistory, include_computed=False))
                - spec.history_omit_fields
            )

            if spec.tenant_aware:
                hist_need = hist_need | {TENANT_ID_FIELD}

            await _require_columns(
                introspector,
                schema=spec.history_relation[0],
                relation=spec.history_relation[1],
                required=hist_need,
                label=f"{spec.name} history",
            )

            await _validate_relation_models(
                introspector,
                schema=spec.history_relation[0],
                relation=spec.history_relation[1],
                models=[
                    (
                        DocumentHistory,
                        spec.history_omit_fields,
                        f"{spec.name} history",
                    ),
                ],
            )

            if spec.tenant_aware:
                await _validate_tenant_column(
                    introspector,
                    schema=spec.history_relation[0],
                    relation=spec.history_relation[1],
                    label=f"{spec.name} history",
                )
