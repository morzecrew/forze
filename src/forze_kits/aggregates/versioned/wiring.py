"""Reusable versioned-facts wiring: current-only reads, the correction command, lineage reads.

The read side is the half that makes the kit worth having. Every generated LIST filters
``is_current = true`` and GET 404s a superseded row, so an aggregate that opted into correction
lineage reads like an aggregate that never had it — the chain is reachable only by asking for it.

Read exclusion for LIST rides the document factory's **mapper** seam, so it is applied at *build*
time (``build_document_registry(spec, mappers=wiring.read_mappers())``); the ops-merge and the GET
override are applied *after* build (``wiring.bind(reg)``). Requires the domain + update command on
the versioning mixins (a type precondition, not magic).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.contracts.mapping import Mapper
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import stored_field_names_for
from forze_kits.aggregates.document.dto import DocumentIdDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.document.value_objects import DocumentDTOs, DocumentMappers
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD
from forze_kits.domain.versioned.constants import (
    IS_CURRENT_FIELD,
    ROOT_ID_FIELD,
    SUPERSEDED_AT_FIELD,
    SUPERSEDES_ID_FIELD,
    VERSION_FIELD,
)
from forze_kits.mapping import PydanticPipelineMapperFactory

from .factories import build_versioned_registry
from .handlers import SeedFirstVersion
from .policy import VersionedPolicy

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentQueryPort, DocumentSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


_REQUIRED_READ_FIELDS: Final = (
    ROOT_ID_FIELD,
    VERSION_FIELD,
    SUPERSEDES_ID_FIELD,
    IS_CURRENT_FIELD,
    SUPERSEDED_AT_FIELD,
)
"""What the read side and the correction command read off a versioned aggregate's read model."""

_KIT_OWNED_FIELDS: Final = frozenset(_REQUIRED_READ_FIELDS)
"""Lineage fields the kit fills on every write, so their absence from a read model is its own
refusal above rather than a carrying problem."""


def _current_only() -> QueryFilterExpression:
    """A fresh ``is_current == True`` equality predicate (never a shared mutable)."""

    return {"$values": {IS_CURRENT_FIELD: True}}


def _merge_current(filters: QueryFilterExpression | None) -> QueryFilterExpression:
    """Conjoin the current-version restriction with the caller's *filters* (if any)."""

    if filters is None:
        return _current_only()

    return {"$and": [_current_only(), filters]}


def _without_lineage(base: Any) -> Any:
    """An update mapper that drops the lineage fields from a caller's patch.

    The update command carries ``is_current`` and ``superseded_at`` because the kit's own retire
    write needs them, and that command is also what the generated ``UPDATE`` accepts — so without
    this a caller can retire the only version of a fact through an ordinary update, leaving no
    current version, no successor and no correction record. Which is the thing the aggregate
    exists to make impossible.

    Dropped rather than refused: a patch that happens to carry a default is not an attack, and a
    caller cannot tell which fields a kit reserves. The correction command writes through the
    port directly, so it is unaffected.
    """

    def _factory(ctx: ExecutionContext) -> Mapper[Any, Any]:
        inner = base(ctx) if base is not None else None

        async def _map(source: Any) -> Any:
            stripped = source.model_copy(
                update=dict.fromkeys(_KIT_OWNED_FIELDS & set(type(source).model_fields), None)
            )
            cleaned = stripped.model_dump(exclude=set(_KIT_OWNED_FIELDS), exclude_unset=True)
            rebuilt = type(source).model_validate(cleaned)

            return await inner(rebuilt) if inner is not None else rebuilt

        return _map

    return _factory


# ....................... #


def _after(base: Any) -> Any:
    """A mapper factory running *base* first, then the current-version restriction.

    Composition rather than replacement, because the list-family mapper slots are shared: the
    soft-deletion arm installs its exclusion on the same ones, and a kit declaring both must
    apply both.
    """

    if base is None:
        return current_versions_only_mapper

    def _factory(ctx: ExecutionContext) -> Mapper[Any, Any]:
        first = base(ctx)
        second = current_versions_only_mapper(ctx)

        async def _map(source: Any) -> Any:
            return await second(await first(source))

        return _map

    return _factory


# ....................... #


def current_versions_only_mapper(ctx: ExecutionContext) -> Mapper[Any, Any]:
    """A request mapper that conjoins the current-version restriction into ``filters``.

    Shape-agnostic across every ``filters``-carrying request DTO, so one mapper serves the whole
    document LIST family and the search request DTOs alike.
    """

    async def _map(source: Any) -> Any:
        return source.model_copy(update={"filters": _merge_current(source.filters)})

    return _map


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CurrentVersionGet[R: BaseModel](Handler[DocumentIdDTO, R]):
    """GET that rejects a superseded version — the read-side restriction for fetch-by-id.

    A superseded row is still addressable, which is the point of correcting rather than
    overwriting; what it is not is the answer to "give me this fact". Reaching an old version is
    what ``history`` and ``as_of`` are for, and both name the fact rather than the version.

    Also rejects a soft-deleted row, because this handler *replaces* the one soft-delete
    installs rather than running after it — an aggregate composing both arms would otherwise
    serve deleted rows, with nothing in either arm's own tests to show it.
    """

    doc: DocumentQueryPort[R]
    """Document query port for the guarded get."""

    soft_deleted: bool = False
    """Whether soft deletion is wired on this aggregate, and so whether to check its flag.

    Off by default, because ``is_deleted`` is an ordinary field name a domain may use for
    something of its own — refusing a row for carrying it would be this handler inventing a
    lifecycle the author did not declare."""

    # ....................... #

    async def __call__(self, args: DocumentIdDTO) -> R:
        row = await self.doc.get(pk=args.id)

        # Fail closed on a read model that does not expose the flag: `versioned_wiring` refuses
        # to build one, so this default is unreachable rather than lenient — and were it
        # reachable, serving a row whose currency cannot be established is the wrong answer.
        if not getattr(row, IS_CURRENT_FIELD, False):
            raise exc.not_found(
                "This version of the fact was superseded — read the fact's current version, or "
                "its history.",
            )

        # Soft deletion, when the aggregate composes both. This override replaces the one
        # soft-delete installed, so without re-checking here the guard is silently dropped and a
        # deleted row is served — the two arms agree on GET and only the last one to bind runs.
        if self.soft_deleted and getattr(row, SOFT_DELETE_FIELD, False):
            raise exc.not_found("Document was deleted")

        return row


# ....................... #


@final
@attrs.frozen(kw_only=True)
class VersionedWiring:
    """The reusable versioned-facts wiring for one document aggregate.

    :meth:`read_mappers` feeds the document factory at build time (LIST restriction); :meth:`bind`
    is applied after build (merges CORRECT/HISTORY/AS_OF, overrides GET).
    """

    spec: DocumentSpec[Any, Any, Any, Any]
    """The versioned document aggregate (domain + update cmd on the versioning mixins)."""

    policy: VersionedPolicy
    """Where correction records are stored."""

    soft_deleted: bool = False
    """Whether the aggregate also composes soft deletion, so the GET guard checks its flag."""

    dtos: DocumentDTOs[Any, Any, Any] | None = None
    """Inbound DTOs, when they are not the spec's own commands."""

    # ....................... #

    def mappers(
        self, base: DocumentMappers[Any, Any, Any, Any] | None = None
    ) -> DocumentMappers[Any, Any, Any, Any]:
        """The document factory's mappers, with what versioning adds to them.

        Two things: the list family restricts reads to current versions, and the update command
        drops lineage fields a caller supplied.

        Runs *after* whatever mapper *base* already carries rather than replacing it: an
        aggregate composing soft deletion has an exclusion mapper on the same slot, and
        overwriting it lists rows that are current and deleted. Each mapper conjoins into the
        filter the previous one produced, so the restrictions accumulate.
        """

        base = base if base is not None else DocumentMappers()

        return attrs.evolve(
            base,
            update=_without_lineage(base.update),
            list=_after(base.list),
            projected_list=_after(base.projected_list),
            cursor_list=_after(base.cursor_list),
            projected_cursor_list=_after(base.projected_cursor_list),
            aggregated_list=_after(base.aggregated_list),
        )

    # ....................... #

    def ops(self, *, ns: StrKeyNamespace | None = None) -> OperationRegistry:
        """The CORRECT + HISTORY + AS_OF ops (empty when the spec is not update-capable)."""

        return build_versioned_registry(self.spec, self.policy, dtos=self.dtos, ns=ns)

    # ....................... #

    def bind(
        self,
        reg: OperationRegistry,
        *,
        ns: StrKeyNamespace | None = None,
    ) -> OperationRegistry:
        """Merge the lineage ops and override the two document ops versioning changes.

        CREATE becomes a first-version insert and GET rejects a superseded row — both override
        an operation the document factory already registered, which is why they are applied here
        rather than merged.
        """

        ns = ns or self.spec.default_namespace
        spec = self.spec
        reg = type(reg).merge(reg, self.ops(ns=ns))

        create_key = ns.key(DocumentKernelOp.CREATE)

        if create_key in reg.operation_keys() and spec.write is not None:
            create_cmd = spec.write["create_cmd"]
            create_dto = (
                self.dtos.create
                if self.dtos is not None and self.dtos.create is not None
                else create_cmd
            )
            seed_mapper = PydanticPipelineMapperFactory(in_=create_dto, out=create_cmd)

            reg = reg.set_handler(
                create_key,
                lambda ctx: SeedFirstVersion(
                    doc=ctx.doc.command(spec),
                    mapper=seed_mapper(ctx),
                ),
                override=True,
            )

        get_key = ns.key(DocumentKernelOp.GET)

        if get_key in reg.operation_keys():
            reg = reg.set_handler(
                get_key,
                lambda ctx: CurrentVersionGet(
                    doc=ctx.doc.query(spec), soft_deleted=self.soft_deleted
                ),
                override=True,
            )

        return reg


# ....................... #


def versioned_wiring(
    spec: DocumentSpec[Any, Any, Any, Any],
    policy: VersionedPolicy,
    *,
    soft_deleted: bool = False,
    dtos: DocumentDTOs[Any, Any, Any] | None = None,
) -> VersionedWiring:
    """Build the reusable versioned-facts wiring for *spec*.

    Refuses at construction on either of two counts, both of which would otherwise surface as a
    wrong answer rather than an error: a spec that does not declare both storage guarantees (the
    kit's correctness rests on them rather than on its own write path), and a read model that
    does not expose everything the kit reads off it.
    """

    VersionedPolicy.assert_guarantees(spec)
    _assert_read_model(spec)

    return VersionedWiring(spec=spec, policy=policy, soft_deleted=soft_deleted, dtos=dtos)


# ....................... #


def _assert_read_model(spec: DocumentSpec[Any, Any, Any, Any]) -> None:
    """Refuse a versioned aggregate whose read model hides what the kit has to read.

    Two separate reasons, both of which produce a *wrong answer* rather than a failure:

    The lineage fields are what the read side and the correction command work from — a read model
    without ``is_current`` leaves the GET guard with nothing to check, and one without ``version``
    leaves the expected-version comparison nothing to compare.

    And a correction builds the successor from the predecessor's **read model**, so a field that
    is persisted but not exposed there cannot be carried across: the successor would silently
    take the create command's default and lose a value the fact asserted. Better to refuse the
    declaration than to lose data on the first correction.

    :raises CoreException: ``configuration`` naming what is missing.
    """

    read_fields = set(spec.read.model_fields)
    missing = [name for name in _REQUIRED_READ_FIELDS if name not in read_fields]

    if missing:
        raise exc.configuration(
            f"Document {spec.name!r} is declared versioned, and its read model does not expose "
            f"{sorted(missing)}. The read side and the correction command work from those "
            "fields; without them a superseded row reads as current and a stale correction "
            "reads as fresh.",
            details={"document": spec.name, "missing": sorted(missing)},
        )

    if spec.write is None:
        return

    carried = stored_field_names_for(spec.write["create_cmd"]) - _KIT_OWNED_FIELDS
    lost = sorted(carried - read_fields)

    if lost:
        raise exc.configuration(
            f"Document {spec.name!r} persists {lost} through its create command without exposing "
            "them on its read model, so a correction could not carry them to the successor — it "
            "builds the new version from the old one's read model, and would silently fall back "
            "to the command's defaults. Expose them, or move them out of the create command.",
            details={"document": spec.name, "not_readable": lost},
        )
