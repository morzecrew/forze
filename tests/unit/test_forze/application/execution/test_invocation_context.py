"""Tests for :mod:`forze.application.execution.context.invocation`."""

from uuid import uuid4

import pytest
from structlog.contextvars import get_contextvars

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context.invocation import (
    ACTOR_ID_KEY,
    CAUS_ID_KEY,
    CORR_ID_KEY,
    EXEC_ID_KEY,
    PRINCIPAL_ID_KEY,
    TENANT_ID_KEY,
    InvocationContext,
    InvocationMetadata,
)

# ----------------------- #


def _metadata() -> InvocationMetadata:
    return InvocationMetadata(
        execution_id=uuid4(),
        correlation_id=uuid4(),
        causation_id=uuid4(),
    )


def _authn() -> AuthnIdentity:
    return AuthnIdentity(
        principal_id=uuid4(),
        actor=AuthnIdentity(principal_id=uuid4()),
    )


def _log_keys() -> set[str]:
    return {
        EXEC_ID_KEY,
        CORR_ID_KEY,
        CAUS_ID_KEY,
        PRINCIPAL_ID_KEY,
        ACTOR_ID_KEY,
        TENANT_ID_KEY,
    }


def _bound_log_vars() -> dict[str, object]:
    return {k: v for k, v in get_contextvars().items() if k in _log_keys()}


# ----------------------- #


def test_bind_matches_bind_metadata_composed_with_bind_identity() -> None:
    """``bind()`` is observably bind_metadata ∘ bind_identity."""

    ctx = InvocationContext()
    metadata = _metadata()
    authn = _authn()
    tenant = TenantIdentity(tenant_id=uuid4())

    with ctx.bind_metadata(metadata=metadata):
        with ctx.bind_identity(authn=authn, tenant=tenant):
            composed = (
                ctx.get_metadata(),
                ctx.get_authn(),
                ctx.get_tenant(),
                _bound_log_vars(),
            )

    assert ctx.get_metadata() is None
    assert ctx.get_authn() is None
    assert ctx.get_tenant() is None
    assert _bound_log_vars() == {}

    with ctx.bind(metadata=metadata, authn=authn, tenant=tenant):
        assert (
            ctx.get_metadata(),
            ctx.get_authn(),
            ctx.get_tenant(),
            _bound_log_vars(),
        ) == composed

        assert _bound_log_vars() == {
            EXEC_ID_KEY: str(metadata.execution_id),
            CORR_ID_KEY: str(metadata.correlation_id),
            CAUS_ID_KEY: str(metadata.causation_id),
            PRINCIPAL_ID_KEY: authn.principal_id,
            ACTOR_ID_KEY: authn.actor.principal_id,  # type: ignore[union-attr]
            TENANT_ID_KEY: str(tenant.tenant_id),
        }

    assert ctx.get_metadata() is None
    assert ctx.get_authn() is None
    assert ctx.get_tenant() is None
    assert _bound_log_vars() == {}


def test_bind_optional_identities_omitted() -> None:
    ctx = InvocationContext()
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.bind(metadata=metadata):
        assert ctx.get_metadata() is metadata
        assert ctx.get_authn() is None
        assert ctx.get_tenant() is None

        assert _bound_log_vars() == {
            EXEC_ID_KEY: str(metadata.execution_id),
            CORR_ID_KEY: str(metadata.correlation_id),
        }


def test_bind_resets_on_exception() -> None:
    ctx = InvocationContext()
    metadata = _metadata()
    authn = _authn()
    tenant = TenantIdentity(tenant_id=uuid4())

    with pytest.raises(RuntimeError, match="boom"):
        with ctx.bind(metadata=metadata, authn=authn, tenant=tenant):
            raise RuntimeError("boom")

    assert ctx.get_metadata() is None
    assert ctx.get_authn() is None
    assert ctx.get_tenant() is None
    assert _bound_log_vars() == {}


def test_bind_nests_and_restores_outer_values() -> None:
    ctx = InvocationContext()
    outer_md = _metadata()
    inner_md = _metadata()
    outer_authn = _authn()

    with ctx.bind(metadata=outer_md, authn=outer_authn):
        with ctx.bind(metadata=inner_md):
            assert ctx.get_metadata() is inner_md
            assert ctx.get_authn() is None

        assert ctx.get_metadata() is outer_md
        assert ctx.get_authn() is outer_authn
        assert _bound_log_vars()[EXEC_ID_KEY] == str(outer_md.execution_id)
