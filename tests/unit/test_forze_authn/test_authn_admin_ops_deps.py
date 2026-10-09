"""The admin API-key operations run through the registry against the identity plane's own deps."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from tests.support.execution_context import context_from_deps

pytest.importorskip("jwt")
pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

from forze.application.contracts.authn import AuthnIdentity, AuthnSpec
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.execution import BeforeStep
from forze.application.execution import Deps
from forze.application.execution.operations import run_operation
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.authn.domain.models.account import ReadApiKeyAccount
from forze_kits.aggregates.authn import (
    AuthnKernelOp,
    AuthnPrincipalRefDTO,
    build_authn_registry,
)

# ----------------------- #

SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))


async def _allow(args: Any) -> None:
    _ = args


async def test_an_admin_lists_another_principals_keys_through_the_wired_lifecycle() -> None:
    # The lifecycle adapter holds the API-key command port, which a QUERY operation may not
    # acquire, so the listing runs as a command.
    owner = uuid4()
    now = datetime.now(tz=UTC)
    account = ReadApiKeyAccount(
        id=uuid4(),
        rev=1,
        created_at=now,
        last_update_at=now,
        principal_id=owner,
        key_hash="h",
        is_active=True,
    )

    def port(ctx: object, spec: object) -> MagicMock:
        doc = MagicMock()
        doc.spec = spec
        doc.find_many = AsyncMock(return_value=MagicMock(hits=[account]))
        return doc

    routes = {AuthnResourceName.API_KEY_ACCOUNTS: port}
    deps = AuthnDepsModule(
        kernel=AuthnKernelConfig(access_token_secret=b"k" * 32, api_key_pepper=b"a" * 32),
        authn={"main": frozenset({"api_key"})},
        api_key_lifecycle={"main"},
        eligibility="allow_all",
    )().merge(Deps.routed({DocumentQueryDepKey: dict(routes), DocumentCommandDepKey: dict(routes)}))
    ctx = context_from_deps(deps)
    registry = build_authn_registry(
        SPEC, admin_guards=(BeforeStep(id="app.admin", factory=lambda ctx: _allow),)
    ).freeze()
    op = SPEC.default_namespace.key(AuthnKernelOp.LIST_PRINCIPAL_API_KEYS)

    with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4())):
        listed = await run_operation(registry, op, AuthnPrincipalRefDTO(id=owner), ctx)

    assert [item.key_id for item in listed.keys] == [account.id]
