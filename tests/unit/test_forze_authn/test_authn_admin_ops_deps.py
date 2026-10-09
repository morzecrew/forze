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
from forze.application.contracts.deps import GuardedWritePort
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.execution import BeforeStep
from forze.application.execution import Deps
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
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


async def test_an_admin_lists_another_principals_keys_as_a_query() -> None:
    # The lifecycle adapter is built with the API-key command port; a query may hold it, and
    # the listing reads through the query port alone.
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
    query_ports: list[MagicMock] = []

    def port(ctx: object, spec: object) -> MagicMock:
        doc = MagicMock()
        doc.spec = spec
        doc.find_many = AsyncMock(return_value=MagicMock(hits=[account]))
        query_ports.append(doc)
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
        SPEC,
        admin_guards=(BeforeStep(id="app.admin", factory=lambda ctx: _allow),),
        trust_admin_guards=True,
    ).freeze()
    op = SPEC.default_namespace.key(AuthnKernelOp.LIST_PRINCIPAL_API_KEYS)

    assert registry.catalog()[op].is_read_only

    with ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=uuid4())):
        listed = await run_operation(registry, op, AuthnPrincipalRefDTO(id=owner), ctx)

        assert [item.key_id for item in listed.keys] == [account.id]

        # A write through the port the query holds is refused, inside the operation.
        held = registry.resolve(op, ctx).handler.api_key_lifecycle.ak_cmd  # type: ignore[union-attr]
        assert isinstance(held, GuardedWritePort)

        async def write_instead(*args: Any, **kwargs: Any) -> Any:
            await held.update(account.id, account.rev, None)

        for doc in query_ports:
            doc.find_many.side_effect = write_instead

        with pytest.raises(CoreException) as caught:
            await run_operation(registry, op, AuthnPrincipalRefDTO(id=owner), ctx)

    assert caught.value.kind is ExceptionKind.PRECONDITION
