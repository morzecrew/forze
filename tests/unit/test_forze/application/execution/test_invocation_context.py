"""Tests for :mod:`forze.application.execution.context.invocation`."""

from uuid import uuid4

import pytest
from structlog.contextvars import bound_contextvars, get_contextvars

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context.invocation import (
    ACTOR_ID_KEY,
    CAUS_ID_KEY,
    CORR_ID_KEY,
    EXEC_ID_KEY,
    IDEMPOTENCY_KEY_KEY,
    PRINCIPAL_ID_KEY,
    SURFACE_KEY,
    TENANT_ID_KEY,
    InvocationContext,
    InvocationMetadata,
    InvocationSurface,
)
from forze.base.exceptions import CoreException, ExceptionKind

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
        SURFACE_KEY,
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

    with ctx.bind_metadata(metadata=metadata), ctx.bind_identity(authn=authn, tenant=tenant):
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
            SURFACE_KEY: "unknown",
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
            SURFACE_KEY: "unknown",
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


def test_bind_with_identity_sans_actor_and_tenant_binds_exact_fields() -> None:
    ctx = InvocationContext()
    metadata = _metadata()
    authn = AuthnIdentity(principal_id=uuid4())  # no actor

    with ctx.bind(metadata=metadata, authn=authn):
        assert _bound_log_vars() == {
            EXEC_ID_KEY: str(metadata.execution_id),
            CORR_ID_KEY: str(metadata.correlation_id),
            CAUS_ID_KEY: str(metadata.causation_id),
            SURFACE_KEY: "unknown",
            PRINCIPAL_ID_KEY: authn.principal_id,
        }

    assert _bound_log_vars() == {}


def test_bind_identity_empty_binds_no_log_fields_and_preserves_outer() -> None:
    """An identity bind with no identity logs none and leaves every other key alone."""

    ctx = InvocationContext()

    with bound_contextvars(custom="outer"):
        with ctx.bind_identity():
            assert ctx.get_authn() is None
            assert ctx.get_tenant() is None
            assert _bound_log_vars() == {}
            assert get_contextvars().get("custom") == "outer"

        assert get_contextvars().get("custom") == "outer"


def test_bind_identity_empty_resets_contextvars_on_exception() -> None:
    ctx = InvocationContext()
    outer_authn = _authn()

    with ctx.bind_identity(authn=outer_authn):
        with pytest.raises(RuntimeError, match="boom"), ctx.bind_identity():
            assert ctx.get_authn() is None
            raise RuntimeError("boom")

        assert ctx.get_authn() is outer_authn

    assert ctx.get_authn() is None


def test_a_nested_identity_never_logs_the_outer_one() -> None:
    ctx = InvocationContext()
    outer = _authn()
    tenant = TenantIdentity(tenant_id=uuid4())
    service = AuthnIdentity(principal_id=uuid4())  # no actor

    with ctx.bind_identity(authn=outer, tenant=tenant):
        before = _bound_log_vars()

        with ctx.bind_identity():
            assert _bound_log_vars() == {}

        with ctx.bind_identity(authn=service):
            assert _bound_log_vars() == {PRINCIPAL_ID_KEY: service.principal_id}

        assert _bound_log_vars() == before

    assert _bound_log_vars() == {}


def test_bind_idempotency_none_binds_no_log_fields() -> None:
    ctx = InvocationContext()

    with bound_contextvars(custom="outer"), ctx.bind_idempotency(None):
        assert ctx.get_idempotency_key() is None
        assert IDEMPOTENCY_KEY_KEY not in get_contextvars()
        assert get_contextvars().get("custom") == "outer"

    assert ctx.get_idempotency_key() is None


def test_bind_idempotency_key_binds_log_field_and_resets() -> None:
    ctx = InvocationContext()

    with ctx.bind_idempotency("idem-1"):
        assert ctx.get_idempotency_key() == "idem-1"
        assert get_contextvars()[IDEMPOTENCY_KEY_KEY] == "idem-1"

    assert ctx.get_idempotency_key() is None
    assert IDEMPOTENCY_KEY_KEY not in get_contextvars()


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


# ....................... #
# The invocation surface (RFC 0079 §5.1, §5.5).


def test_the_surface_defaults_to_unknown() -> None:
    assert InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()).surface == "unknown"


@pytest.mark.parametrize("surface", list(InvocationSurface))
def test_every_well_known_surface_is_a_valid_token(surface: InvocationSurface) -> None:
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4(), surface=surface)

    assert metadata.surface == surface.value


@pytest.mark.parametrize("surface", ["grpc", "billing_worker", "q2", "a" * 32])
def test_an_application_boundary_passes_its_own_token(surface: str) -> None:
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4(), surface=surface)

    assert metadata.surface == surface


@pytest.mark.parametrize(
    "surface",
    ["", "HTTP", "1cli", "_cli", "partner-api", "cli ", "http\n", "a" * 33, "é", None, 7],
)
def test_a_token_outside_the_pattern_is_a_configuration_error(surface: object) -> None:
    with pytest.raises(CoreException) as caught:
        InvocationMetadata(
            execution_id=uuid4(),
            correlation_id=uuid4(),
            surface=surface,  # type: ignore[arg-type]
        )

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == "invocation_surface_invalid"


def test_the_surface_is_logged_under_its_own_key_and_restored() -> None:
    ctx = InvocationContext()
    outer = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4(), surface="http")
    inner = InvocationMetadata(
        execution_id=uuid4(), correlation_id=uuid4(), surface=InvocationSurface.TEMPORAL
    )

    with ctx.bind_metadata(metadata=outer):
        assert get_contextvars()[SURFACE_KEY] == "http"

        with ctx.bind(metadata=inner):
            assert get_contextvars()[SURFACE_KEY] == "temporal"

        assert get_contextvars()[SURFACE_KEY] == "http"

    assert SURFACE_KEY not in get_contextvars()
