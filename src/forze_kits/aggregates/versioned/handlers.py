"""The correction command and the two lineage reads."""

from typing import Any, cast
from uuid import UUID

import attrs
from pydantic import BaseModel as BM

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.querying import QueryFilterExpression
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze.domain.constants import ID_FIELD, REV_FIELD
from forze.domain.models import BaseDTO, ReadDocument
from forze_kits.domain.versioned.constants import (
    IS_CURRENT_FIELD,
    ROOT_ID_FIELD,
    SUPERSEDED_AT_FIELD,
    SUPERSEDES_ID_FIELD,
    VERSION_FIELD,
)
from forze_kits.domain.versioned.correction import CreateCorrectionCmd
from forze_kits.domain.versioned.models import (
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_kits.dto.paginated import Paginated

from .dto import CorrectDocumentDTO, FactAsOfDTO, FactIdDTO

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SeedFirstVersion[In: BaseDTO, Cmd: BaseDTO, Out: BM](Handler[In, Out]):
    """CREATE that makes the new row the first version of a new fact.

    ``root_id`` is the fact and ``id`` is this version of it; on a first insert they are the same
    value, which is why the id is chosen here rather than left to the store — the payload has to
    carry the identity the row is about to be given.

    The lineage fields are overwritten rather than defaulted, so a caller that supplied them
    cannot declare its new row to be version 7 of somebody else's fact. Changing what a fact says
    is what ``correct`` is for, and it is the only path that writes a non-trivial lineage.
    """

    doc: DocumentCommandPort[Out, Any, Cmd, Any]
    """Document port for create operations."""

    mapper: Any
    """The document factory's create mapper, converting the input DTO to a create command."""

    # ....................... #

    async def __call__(self, args: In) -> Out:
        """Create the first version of a fact.

        :param args: Input DTO (e.g. request payload).
        :returns: The created read model.
        """

        cmd = await self.mapper(args)
        # `uuid7`, not `uuid4`: it is the id factory every document already uses, so a seeded
        # row sorts with its siblings, and it routes through the entropy seam a simulation
        # replays. A raw `uuid4` here would make a versioned aggregate unreproducible.
        fact_id = uuid7()

        seeded = cmd.model_copy(
            update={
                ROOT_ID_FIELD: fact_id,
                VERSION_FIELD: 1,
                SUPERSEDES_ID_FIELD: None,
                IS_CURRENT_FIELD: True,
                SUPERSEDED_AT_FIELD: None,
            }
        )

        return await self.doc.create(cast(Cmd, seeded), id=fact_id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CorrectDocument[Out: BM, D: DocWithVersioning, C: BaseDTO, U: UpdateCmdWithVersioning](
    Handler[CorrectDocumentDTO[Any], Out]
):
    """Supersede the current version of a fact with a corrected one.

    Four writes in the caller's transaction:

    1. read the predecessor and refuse unless it is current and at the expected version;
    2. **retire the predecessor**;
    3. insert the successor, carrying the patch, ``version + 1`` and ``supersedes_id``;
    4. record the correction.

    Retire *before* insert, which is the opposite of the obvious order and the only one the
    declared guarantee permits: ``UniqueTogether(("root_id",), where=is_current)`` means a fact
    may not have two current rows, and inserting first creates exactly that — the guarantee
    refuses the successor and no correction is possible at all. Retiring first passes through
    "no current version" instead, which is a state the transaction closes and a concurrent
    reader never observes.

    The actor is read from the invocation's identity, never from the caller's payload: a record of
    who changed a fact that the changer can write is not a record of anything.
    """

    doc: DocumentCommandPort[Out, D, C, U]
    """Command port for the versioned aggregate."""

    query: DocumentQueryPort[Out]
    """Query port for the versioned aggregate, used to read the predecessor."""

    corrections: DocumentCommandPort[Any, Any, CreateCorrectionCmd, Any]
    """Command port for the correction records."""

    create_cmd: type[C]
    """The aggregate's create command — the successor is built with it, not with the patch."""

    actor: Any
    """``ctx.inv_ctx.get_authn`` — the invocation's identity, read at call time."""

    # ....................... #

    async def __call__(self, args: CorrectDocumentDTO[Any]) -> Out:
        """Correct the fact *args.id* asserts.

        :param args: The version read, the version number asserted, the patch and the reason.
        :returns: The successor's read model.
        :raises CoreException: ``conflict`` when the row is not current or not at the expected
            version — the same kind a unique-index violation raises, so a caller retrying or
            reporting a conflict needs to know neither which check fired nor which store it spoke
            to.
        """

        predecessor = await self.query.get(pk=args.id)
        self._require_correctable(predecessor, args.expected_version)

        successor_id = uuid7()
        root_id = getattr(predecessor, ROOT_ID_FIELD)
        version = int(getattr(predecessor, VERSION_FIELD))

        # One clock read for both sides of the handover: the predecessor's end and the
        # successor's start are the same instant by definition, and two reads leave a gap in
        # which `as_of` matches neither version and reports the fact never existed.
        at = utcnow()

        upd_cls = cast(type[U], UpdateCmdWithVersioning)
        # Rev-guarded on the predecessor as it was read: the version check above settles the
        # lineage race, and this settles the ordinary one — a concurrent writer that touched the
        # predecessor between the read and here loses rather than being silently overwritten.
        await self.doc.update(
            pk=args.id,
            rev=int(getattr(predecessor, REV_FIELD)),
            dto=upd_cls(is_current=False, superseded_at=at),
        )

        successor = await self.doc.create(
            self._successor_of(predecessor, args, root_id=root_id, version=version),
            id=successor_id,
        )

        identity = self.actor()
        await self.corrections.create(
            CreateCorrectionCmd(
                root_id=root_id,
                from_id=args.id,
                to_id=successor_id,
                actor_id=identity.principal_id if identity is not None else None,
                reason=args.reason,
            ),
        )

        return successor

    # ....................... #

    def _successor_of(
        self,
        predecessor: Any,
        args: CorrectDocumentDTO[Any],
        *,
        root_id: UUID,
        version: int,
    ) -> C:
        """The corrected version: the predecessor's values, the caller's patch over them.

        A correction is a *new assertion of the whole fact*, not a delta stored against an old
        one — so the successor has to carry every field, or reading the current version would
        mean walking the chain and replaying patches, which is the anti-join mistake in another
        costume. The patch supplies only what changed (``exclude_unset``, so an explicit ``None``
        still clears a field while an omitted one inherits).

        Filtered to the create command's own fields: a read model may carry derived or computed
        values that were never stored and that the command has nowhere to put.
        """

        fields = set(self.create_cmd.model_fields)
        carried = {
            name: value for name, value in predecessor.model_dump().items() if name in fields
        }
        patch = {
            name: value
            for name, value in args.dto.model_dump(exclude_unset=True).items()
            if name in fields
        }

        return self.create_cmd(
            **carried
            | patch
            | {
                ROOT_ID_FIELD: root_id,
                VERSION_FIELD: version + 1,
                SUPERSEDES_ID_FIELD: args.id,
                IS_CURRENT_FIELD: True,
                SUPERSEDED_AT_FIELD: None,
            }
        )

    # ....................... #

    @staticmethod
    def _require_correctable(row: Any, expected_version: int) -> None:
        """Refuse a correction of a row that is not the fact's current assertion.

        Both refusals are ``conflict`` rather than ``precondition``: the caller read a version and
        acted on it, and something else changed the fact first. That is the same story a unique
        violation tells, and a boundary renders it 409 either way.
        """

        if not getattr(row, IS_CURRENT_FIELD, False):
            raise exc.conflict(
                "Cannot correct a superseded version — read the fact's current version and "
                "correct that.",
                details={"id": str(getattr(row, ID_FIELD, "")), "reason": "not_current"},
            )

        version = int(getattr(row, VERSION_FIELD))

        if version != expected_version:
            raise exc.conflict(
                f"Expected version {expected_version}, found {version} — the fact was corrected "
                "since it was read.",
                details={
                    "id": str(getattr(row, ID_FIELD, "")),
                    "expected_version": expected_version,
                    "version": version,
                },
            )


# ....................... #


def _of_fact(root_id: UUID) -> QueryFilterExpression:
    """Every version of one fact."""

    return {"$values": {ROOT_ID_FIELD: root_id}}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FactHistory[Out: BM](Handler[FactIdDTO, Paginated[Out]]):
    """Every version of a fact, oldest first.

    Returned in the framework's list envelope rather than as a bare list, so the chain reads
    through the same shape as every other multi-row answer and a transport needs no special case
    for it.
    """

    query: DocumentQueryPort[Out]
    """Query port for the versioned aggregate."""

    # ....................... #

    async def __call__(self, args: FactIdDTO) -> Paginated[Out]:
        """Return the fact's chain in version order.

        :param args: The fact.
        :returns: Its versions, oldest first.
        """

        page = await self.query.find_page(
            filters=_of_fact(args.root_id),
            sorts={VERSION_FIELD: "asc"},
        )

        return Paginated.from_page(page)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FactAsOf[Out: ReadDocument](Handler[FactAsOfDTO, Out]):
    """The version of a fact that was current at an instant.

    Reads only stored columns — ``created_at`` and ``superseded_at`` — so it answers "what did we
    believe then", not "what was true then". The second question is validity over time and a
    different declaration entirely.
    """

    query: DocumentQueryPort[Out]
    """Query port for the versioned aggregate."""

    # ....................... #

    async def __call__(self, args: FactAsOfDTO) -> Out:
        """Return the version current at *args.at*.

        A version's window runs from its own ``created_at`` until its **successor's**, half-open,
        so consecutive versions tile exactly and an instant equal to a correction belongs to the
        successor alone.

        Deliberately not ``superseded_at``: that records when the retirement was *written*, which
        is a moment before the successor is inserted. Reading the window from it leaves the
        interval between the two writes matching no version at all, so a report asks what a fact
        said at an instant and is told the fact did not exist.

        :param args: The fact and the instant.
        :returns: The version current then.
        :raises CoreException: ``not_found`` when the fact had no version at that instant.
        """

        page = await self.query.find_page(
            filters=_of_fact(args.root_id),
            sorts={VERSION_FIELD: "asc"},
        )
        chain = list(page.hits)

        for position, row in enumerate(chain):
            if row.created_at > args.at:
                continue

            successor = chain[position + 1] if position + 1 < len(chain) else None

            if successor is None or args.at < successor.created_at:
                return row

        raise exc.not_found(
            f"No version of this fact was current at {args.at.isoformat()}.",
            details={"root_id": str(args.root_id)},
        )
