"""Tests for :class:`~forze_identity.authn.resolvers.mapping_table.MappingTableResolver`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import VerifiedAssertion
from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException, exc
from forze_identity.authn.domain.models.identity_mapping import (
    CreateIdentityMappingCmd,
    ReadIdentityMapping,
)
from forze_identity.authn.resolvers.mapping_table import MappingTableResolver


def _spec() -> DocumentSpec:
    return DocumentSpec(name="identity_mappings", read=ReadIdentityMapping)


def test_post_init_rejects_cache_and_history() -> None:
    qry = MagicMock()
    qry.spec = DocumentSpec(
        name="m",
        read=ReadIdentityMapping,
        cache=CacheSpec(name="cache"),
    )
    with pytest.raises(CoreException, match="caching is forbidden"):
        MappingTableResolver(qry=qry)

    qry.spec = DocumentSpec(
        name="m",
        read=ReadIdentityMapping,
        history_enabled=True,
    )
    with pytest.raises(CoreException, match="history is forbidden"):
        MappingTableResolver(qry=qry)


def test_post_init_requires_cmd_when_provisioning() -> None:
    qry = MagicMock()
    qry.spec = _spec()
    with pytest.raises(CoreException, match="command port"):
        MappingTableResolver(qry=qry, provision_on_first_sight=True)


@pytest.mark.asyncio
async def test_resolve_existing_mapping() -> None:
    pid = uuid4()
    row = MagicMock()
    row.principal_id = pid
    qry = MagicMock()
    qry.spec = _spec()
    qry.find = AsyncMock(return_value=row)

    resolver = MappingTableResolver(qry=qry)
    out = await resolver.resolve(
        VerifiedAssertion(issuer="iss", subject="sub"),
    )
    assert out.principal_id == pid


@pytest.mark.asyncio
async def test_resolve_unknown_subject_without_provision_raises() -> None:
    qry = MagicMock()
    qry.spec = _spec()
    qry.find = AsyncMock(return_value=None)

    resolver = MappingTableResolver(qry=qry, provision_on_first_sight=False)
    with pytest.raises(CoreException, match="No identity mapping"):
        await resolver.resolve(
            VerifiedAssertion(issuer="iss", subject="sub"),
        )


@pytest.mark.asyncio
async def test_resolve_provisions_on_first_sight() -> None:
    qry = MagicMock()
    qry.spec = _spec()
    qry.find = AsyncMock(return_value=None)
    cmd = MagicMock()
    cmd.create = AsyncMock(return_value=None)

    resolver = MappingTableResolver(
        qry=qry,
        cmd=cmd,
        provision_on_first_sight=True,
    )
    out = await resolver.resolve(
        VerifiedAssertion(issuer="iss", subject="new"),
    )
    cmd.create.assert_awaited_once()
    call_args = cmd.create.await_args
    assert call_args is not None
    dto: CreateIdentityMappingCmd = call_args.args[0]
    assert dto.issuer == "iss"
    assert dto.subject == "new"
    assert out.principal_id == dto.principal_id


@pytest.mark.asyncio
async def test_resolve_provision_retries_after_create_conflict() -> None:
    pid = uuid4()
    raced = MagicMock()
    raced.principal_id = pid

    qry = MagicMock()
    qry.spec = _spec()
    qry.find = AsyncMock(side_effect=[None, raced])
    cmd = MagicMock()
    cmd.create = AsyncMock(
        side_effect=exc.conflict("Duplicate key violation", code="conflict"),
    )

    resolver = MappingTableResolver(
        qry=qry,
        cmd=cmd,
        provision_on_first_sight=True,
    )
    out = await resolver.resolve(
        VerifiedAssertion(issuer="iss", subject="new"),
    )

    assert out.principal_id == pid
    assert qry.find.await_count == 2
    cmd.create.assert_awaited_once()
