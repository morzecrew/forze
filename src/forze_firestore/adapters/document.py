"""Firestore-backed document adapter implementing read and write port contracts."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from typing import Literal, Sequence, TypeVar, final, overload

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.coordinators import DocumentCacheCoordinator, DocumentCoordinator
from forze.base.errors import CoreError
from forze.base.serialization import (
    pydantic_dump,
    pydantic_dump_many,
    pydantic_validate,
    pydantic_validate_many,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..kernel.gateways import FirestoreReadGateway, FirestoreWriteGateway

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreDocumentAdapter(DocumentCoordinator[R, D, C, U]):
    """Firestore adapter bridging domain document ports to gateway operations."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""

    read_gw: FirestoreReadGateway[R]
    """Gateway used for all read queries."""

    write_gw: FirestoreWriteGateway[D, C, U] | None = attrs.field(default=None)
    """Optional gateway for mutations; ``None`` disables write operations."""

    cache_coord: DocumentCacheCoordinator[R]
    """Unified read/write cache semantics for documents."""

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads when pagination omits ``limit``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        if self.write_gw is not None:
            if self.write_gw.client is not self.read_gw.client:
                raise CoreError("Write and read gateways must use the same client")

            if self.write_gw.tenant_aware != self.read_gw.tenant_aware:
                raise CoreError(
                    "Write and read gateways must have the same tenant awareness."
                )

    # ....................... #

    @overload
    async def create(self, dto: C, *, return_new: Literal[True] = True) -> R: ...

    @overload
    async def create(self, dto: C, *, return_new: Literal[False]) -> None: ...

    async def create(self, dto: C, *, return_new: bool = True) -> R | None:
        """Create without a post-write read inside Firestore transactions."""

        write_gw = self.write_gw

        if (
            write_gw is None
            or not return_new
            or not write_gw.client.is_in_transaction()
        ):
            return await super().create(dto, return_new=return_new)  # type: ignore[call-overload]

        domain = await write_gw.create(dto)
        await self.cache_coord.invalidate_keys_now(domain.id)

        res = pydantic_validate(self.spec.read, pydantic_dump(domain))

        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_one(res)
        )

        return res

    # ....................... #

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Sequence[R]: ...

    @overload
    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> None: ...

    async def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Sequence[R] | None:
        write_gw = self.write_gw

        if (
            write_gw is None
            or not return_new
            or not write_gw.client.is_in_transaction()
        ):
            return await super().create_many(dtos, return_new=return_new)  # type: ignore[call-overload]

        if not dtos:
            return []

        domains = await write_gw.create_many(dtos, batch_size=self.batch_size)
        pks_new = [doc.id for doc in domains]
        await self.cache_coord.invalidate_keys_now(*pks_new)

        res = pydantic_validate_many(self.spec.read, pydantic_dump_many(domains))

        await self.cache_coord.after_commit_or_now(
            lambda: self.cache_coord.set_many(res)
        )
        return res
