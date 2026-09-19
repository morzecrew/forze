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

from typing import TYPE_CHECKING, Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.contracts.mapping import Mapper
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.document.dto import DocumentIdDTO
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.aggregates.document.value_objects import DocumentDTOs, DocumentMappers
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD
from forze_kits.domain.versioned.constants import IS_CURRENT_FIELD
from forze_kits.mapping import PydanticPipelineMapperFactory

from .factories import build_versioned_registry
from .handlers import SeedFirstVersion
from .policy import VersionedPolicy

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentQueryPort, DocumentSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def _current_only() -> QueryFilterExpression:
    """A fresh ``is_current == True`` equality predicate (never a shared mutable)."""

    return {"$values": {IS_CURRENT_FIELD: True}}


def _merge_current(filters: QueryFilterExpression | None) -> QueryFilterExpression:
    """Conjoin the current-version restriction with the caller's *filters* (if any)."""

    if filters is None:
        return _current_only()

    return {"$and": [_current_only(), filters]}


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

    # ....................... #

    async def __call__(self, args: DocumentIdDTO) -> R:
        row = await self.doc.get(pk=args.id)

        if not getattr(row, IS_CURRENT_FIELD, True):
            raise exc.not_found(
                "This version of the fact was superseded — read the fact's current version, or "
                "its history.",
            )

        # Soft deletion, when the aggregate composes both. This override replaces the one
        # soft-delete installed, so without re-checking here the guard is silently dropped and a
        # deleted row is served — the two arms agree on GET and only the last one to bind runs.
        if getattr(row, SOFT_DELETE_FIELD, False):
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

    # ....................... #

    def read_mappers(
        self, base: DocumentMappers[Any, Any, Any, Any] | None = None
    ) -> DocumentMappers[Any, Any, Any, Any]:
        """List mappers that restrict reads to current versions — pass to the document factory.

        Overrides the list-family mappers on *base* (create/update mappers are preserved).
        """

        base = base if base is not None else DocumentMappers()

        return attrs.evolve(
            base,
            list=current_versions_only_mapper,
            projected_list=current_versions_only_mapper,
            cursor_list=current_versions_only_mapper,
            projected_cursor_list=current_versions_only_mapper,
            aggregated_list=current_versions_only_mapper,
        )

    # ....................... #

    def ops(self, *, ns: StrKeyNamespace | None = None) -> OperationRegistry:
        """The CORRECT + HISTORY + AS_OF ops (empty when the spec is not update-capable)."""

        return build_versioned_registry(self.spec, self.policy, ns=ns)

    # ....................... #

    def bind(
        self,
        reg: OperationRegistry,
        *,
        dtos: DocumentDTOs[Any, Any, Any] | None = None,
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
            create_dto = dtos.create if dtos is not None and dtos.create is not None else create_cmd
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
                lambda ctx: CurrentVersionGet(doc=ctx.doc.query(spec)),
                override=True,
            )

        return reg


# ....................... #


def versioned_wiring(
    spec: DocumentSpec[Any, Any, Any, Any],
    policy: VersionedPolicy,
) -> VersionedWiring:
    """Build the reusable versioned-facts wiring for *spec*.

    Refuses at construction unless the spec declares both storage guarantees: the kit's
    correctness rests on them rather than on its own write path, so a versioned aggregate that
    could reach a store without them is one this kit must not build.
    """

    VersionedPolicy.assert_guarantees(spec)

    return VersionedWiring(spec=spec, policy=policy)
