"""Tests for the document-backed delegation (``may_act``) adapters."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.base import Page
from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import CoreException
from forze_identity.authz.adapters.delegation import (
    DelegationGrantAdapter,
    DelegationQueryAdapter,
)
from forze_identity.authz.domain.models.bindings import ReadDelegationGrant
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal

pytestmark = pytest.mark.unit


def _secure_spec(name: str, model: type) -> DocumentSpec:
    return DocumentSpec(name=name, read=model)


def _read_grant(actor_id, subject_id) -> ReadDelegationGrant:  # noqa: ANN001
    now = datetime.now(tz=timezone.utc)
    return ReadDelegationGrant(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        actor_id=actor_id,
        subject_id=subject_id,
    )


def _query_adapter(find_return: object) -> DelegationQueryAdapter:
    grant_qry = MagicMock()
    grant_qry.spec = _secure_spec("delegation_grants", ReadDelegationGrant)
    grant_qry.find = AsyncMock(return_value=find_return)
    return DelegationQueryAdapter(
        spec=AuthzSpec(name="main"),
        grant_qry=grant_qry,  # type: ignore[arg-type]
    )


def _grant_adapter(*, principal_exists: bool = True) -> DelegationGrantAdapter:
    now = datetime.now(tz=timezone.utc)
    principal = ReadPolicyPrincipal(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        kind="user",
        is_active=True,
    )

    principal_qry = MagicMock()
    principal_qry.spec = _secure_spec("principals", ReadPolicyPrincipal)
    principal_qry.find = AsyncMock(return_value=principal if principal_exists else None)

    grant_qry = MagicMock()
    grant_qry.spec = _secure_spec("delegation_grants", ReadDelegationGrant)
    grant_qry.find = AsyncMock(return_value=None)
    grant_qry.find_many = AsyncMock(
        return_value=Page(hits=[], count=0, page=1, size=500),
    )

    grant_cmd = MagicMock()
    grant_cmd.spec = _secure_spec("delegation_grants_cmd", ReadDelegationGrant)
    grant_cmd.create = AsyncMock()
    grant_cmd.kill = AsyncMock()

    return DelegationGrantAdapter(
        spec=AuthzSpec(name="main"),
        principal_qry=principal_qry,  # type: ignore[arg-type]
        grant_qry=grant_qry,  # type: ignore[arg-type]
        grant_cmd=grant_cmd,  # type: ignore[arg-type]
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_may_act_true_when_grant_exists() -> None:
    actor, subject = uuid4(), uuid4()
    adapter = _query_adapter(_read_grant(actor, subject))

    assert await adapter.may_act(actor, subject) is True


@pytest.mark.asyncio
async def test_may_act_false_when_no_grant() -> None:
    adapter = _query_adapter(None)

    assert await adapter.may_act(uuid4(), uuid4()) is False


@pytest.mark.asyncio
async def test_grant_delegation_creates_binding() -> None:
    adapter = _grant_adapter()

    await adapter.grant_delegation(AuthnIdentity(principal_id=uuid4()), uuid4())

    adapter.grant_cmd.create.assert_awaited_once()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_grant_delegation_idempotent_when_exists() -> None:
    adapter = _grant_adapter()
    actor, subject = uuid4(), uuid4()
    adapter.grant_qry.find = AsyncMock(  # type: ignore[method-assign]
        return_value=_read_grant(actor, subject),
    )

    await adapter.grant_delegation(actor, subject)

    adapter.grant_cmd.create.assert_not_awaited()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_grant_delegation_unknown_principal_raises() -> None:
    adapter = _grant_adapter(principal_exists=False)

    with pytest.raises(CoreException, match="Policy principal not found"):
        await adapter.grant_delegation(uuid4(), uuid4())


@pytest.mark.asyncio
async def test_revoke_delegation_kills_binding() -> None:
    adapter = _grant_adapter()
    actor, subject = uuid4(), uuid4()
    grant = _read_grant(actor, subject)
    adapter.grant_qry.find = AsyncMock(return_value=grant)  # type: ignore[method-assign]

    await adapter.revoke_delegation(actor, subject)

    adapter.grant_cmd.kill.assert_awaited_once_with(grant.id)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_list_delegators_returns_subject_ids() -> None:
    adapter = _grant_adapter()
    actor = uuid4()
    subjects = [uuid4(), uuid4()]
    adapter.grant_qry.find_many = AsyncMock(  # type: ignore[method-assign]
        return_value=Page(
            hits=[_read_grant(actor, s) for s in subjects],
            count=2,
            page=1,
            size=500,
        ),
    )

    result = await adapter.list_delegators(actor)

    assert result == frozenset(subjects)
