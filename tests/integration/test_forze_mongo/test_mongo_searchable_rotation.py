"""Integration test: searchable (deterministic) dual-key rotation end-to-end (real Mongo).

Walks the full rotation lifecycle: write under the old root, switch to the overlap
(new primary + old previous) so queries match both, re-index every row under the new
root, then drop the old root — queries still match. Proves equality search survives a
deterministic-key rotation without downtime.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.application.integrations.crypto import reencrypt_documents
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps

# ----------------------- #

_OLD = b"rotation-old-root-secret-0123456789"
_NEW = b"rotation-new-root-secret-0123456789"


class _Person(Document):
    name: str
    email: str


class _PersonCreate(CreateDocumentCmd):
    name: str
    email: str


class _PersonUpdate(BaseDTO):
    name: str | None = None
    email: str | None = None


class _PersonRead(ReadDocument):
    name: str
    email: str


_SPEC = DocumentSpec(
    name="people_ns",
    read=_PersonRead,
    write={  # type: ignore[arg-type]
        "domain": _Person,
        "create_cmd": _PersonCreate,
        "update_cmd": _PersonUpdate,
    },
    searchable_fields=frozenset({"email"}),
)


def _ctx(
    mongo_client: MongoClient,
    db: str,
    collection: str,
    *,
    root: bytes,
    previous_root: bytes | None = None,
) -> ExecutionContext:
    fac = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )
    deps = Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="people-cmk")),
            deterministic_root=root,
            deterministic_previous_root=previous_root,
        )(),
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        ),
    )
    return context_from_deps(deps)


async def _find_names(ctx: ExecutionContext, email: str) -> set[str]:
    page = await ctx.document.query(_SPEC).find_page(
        filters={"$values": {"email": email}},
    )
    return {hit.name for hit in page.hits}


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_searchable_key_rotation_lifecycle(
    mongo_client: MongoClient,
) -> None:
    db = (await mongo_client.db()).name
    collection = f"people_{uuid4().hex[:8]}"

    # 1. Pre-rotation: Alice written under the OLD root.
    old_ctx = _ctx(mongo_client, db, collection, root=_OLD)
    await old_ctx.document.command(_SPEC).create(
        _PersonCreate(name="Alice", email="shared@example.com")
    )
    assert await _find_names(old_ctx, "shared@example.com") == {"Alice"}

    # 2. Overlap: NEW primary + OLD previous. The old row still matches, and a new
    #    row (Bob, written under NEW) matches the same query too.
    rot_ctx = _ctx(mongo_client, db, collection, root=_NEW, previous_root=_OLD)
    await rot_ctx.document.command(_SPEC).create(
        _PersonCreate(name="Bob", email="shared@example.com")
    )
    assert await _find_names(rot_ctx, "shared@example.com") == {"Alice", "Bob"}

    # 3. Re-index every row under the NEW root (a maintenance sweep).
    count = await reencrypt_documents(
        rot_ctx.document.query(_SPEC),
        rot_ctx.document.command(_SPEC),
        to_update=lambda d: _PersonUpdate(email=d.email),
    )
    assert count == 2

    # 4. Drop the old root — queries still match both, proving the re-index moved
    #    Alice onto the new key (a new-only codec can't see old-key ciphertext).
    new_ctx = _ctx(mongo_client, db, collection, root=_NEW)
    assert await _find_names(new_ctx, "shared@example.com") == {"Alice", "Bob"}
