"""Integration tests for authn against Postgres document gateways."""

from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

pytest.importorskip("argon2")
pytest.importorskip("jwt")

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnSpec,
    PasswordCredentials,
    TokenLifecycleDepKey,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_identity.authn import (
    Argon2PasswordVerifier,
    AuthnOrchestrator,
    ForzeJwtTokenVerifier,
    HmacApiKeyVerifier,
    JwtNativeUuidResolver,
)
from forze_identity.authn.adapters import (
    ApiKeyLifecycleAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    PolicyPrincipalEligibilityAdapter,
    TokenLifecycleAdapter,
)
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    session_spec,
)
from forze_identity.authn.domain.models.account import CreatePasswordAccountCmd
from forze_identity.authz.application import policy_principal_spec
from forze_identity.authz.application.constants import AuthzResourceName
from tests.support.authn_pg_fixtures import (
    create_authn_tables,
    insert_policy_principal_row,
)
from forze_identity.authn.execution import (
    AuthnDepsModule,
    AuthnKernelConfig,
    build_authn_shared_services,
)
from forze_identity.authn.services import (
    AccessTokenService,
    ApiKeyConfig,
    ApiKeyService,
    PasswordConfig,
    PasswordService,
    RefreshTokenConfig,
    RefreshTokenService,
)
from forze_postgres.execution.deps import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
)
from forze_postgres.execution.deps.configs import (
    PostgresDocumentConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #


async def _authn_pg_setup(
    pg_client: PostgresClient,
    *,
    suffix: str,
) -> ExecutionContext:
    """Create authn DDL and routed Postgres document adapters."""

    await create_authn_tables(pg_client, suffix=suffix)
    return context_from_deps(_authn_pg_deps(pg_client, suffix=suffix))


def _authn_pg_deps(pg_client: PostgresClient, *, suffix: str) -> Deps:
    """Postgres document routes for authn integration (DDL must exist)."""

    policy_pri = f"authz_pri_{suffix}"
    pwd = f"authn_pwd_{suffix}"
    ak = f"authn_ak_{suffix}"
    sess = f"authn_sess_{suffix}"

    policy_ro = PostgresReadOnlyDocumentConfig(read=("public", policy_pri))
    pwd_cfg = PostgresDocumentConfig(
        read=("public", pwd),
        write=("public", pwd),
        bookkeeping_strategy="application",
    )
    ak_cfg = PostgresDocumentConfig(
        read=("public", ak),
        write=("public", ak),
        bookkeeping_strategy="application",
    )
    sess_cfg = PostgresDocumentConfig(
        read=("public", sess),
        write=("public", sess),
        bookkeeping_strategy="application",
    )

    introspector = PostgresIntrospector(client=pg_client)

    policy_cmd_cfg = PostgresDocumentConfig(
        read=("public", policy_pri),
        write=("public", policy_pri),
        bookkeeping_strategy="application",
    )

    query_routes = {
        AuthzResourceName.POLICY_PRINCIPALS: ConfigurablePostgresReadOnlyDocument(
            config=policy_ro,
        ),
        AuthnResourceName.PASSWORD_ACCOUNTS: ConfigurablePostgresDocument(
            config=pwd_cfg
        ),
        AuthnResourceName.API_KEY_ACCOUNTS: ConfigurablePostgresDocument(config=ak_cfg),
        AuthnResourceName.TOKEN_SESSIONS: ConfigurablePostgresDocument(config=sess_cfg),
    }

    cmd_routes = {
        AuthzResourceName.POLICY_PRINCIPALS: ConfigurablePostgresDocument(
            config=policy_cmd_cfg,
        ),
        AuthnResourceName.PASSWORD_ACCOUNTS: ConfigurablePostgresDocument(
            config=pwd_cfg
        ),
        AuthnResourceName.API_KEY_ACCOUNTS: ConfigurablePostgresDocument(config=ak_cfg),
        AuthnResourceName.TOKEN_SESSIONS: ConfigurablePostgresDocument(config=sess_cfg),
    }

    return Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: introspector,
        },
    ).merge(
        Deps.routed(
            {
                DocumentQueryDepKey: query_routes,
                DocumentCommandDepKey: cmd_routes,
            },
        ),
    )


def _eligibility(ctx: ExecutionContext) -> PolicyPrincipalEligibilityAdapter:
    return PolicyPrincipalEligibilityAdapter(
        principal_qry=ctx.document.query(policy_principal_spec),
    )


def _invocation_metadata() -> InvocationMetadata:
    return InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())


def _orchestrator(
    *,
    eligibility: PolicyPrincipalEligibilityAdapter,
    password_svc: PasswordService | None = None,
    pa_qry: object = None,
    api_key_svc: ApiKeyService | None = None,
    ak_qry: object = None,
    access_svc: AccessTokenService | None = None,
    methods: frozenset[str],
) -> AuthnOrchestrator:
    """Manually compose an :class:`AuthnOrchestrator` for explicit-wiring tests."""

    return AuthnOrchestrator(
        resolver=JwtNativeUuidResolver(),
        eligibility=eligibility,
        enabled_methods=methods,
        password_verifier=(
            Argon2PasswordVerifier(password_svc=password_svc, pa_qry=pa_qry)  # type: ignore[arg-type]
            if password_svc is not None and pa_qry is not None
            else None
        ),
        api_key_verifier=(
            HmacApiKeyVerifier(api_key_svc=api_key_svc, ak_qry=ak_qry)  # type: ignore[arg-type]
            if api_key_svc is not None and ak_qry is not None
            else None
        ),
        token_verifier=(
            ForzeJwtTokenVerifier(access_svc=access_svc)
            if access_svc is not None
            else None
        ),
    )


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_password_authentication(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pwd_svc = PasswordService()
    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    hashed = pwd_svc.hash_password("correct horse battery staple")
    pwd_cmd = ctx.document.command(password_account_spec)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await pwd_cmd.create(
            CreatePasswordAccountCmd(
                principal_id=pid,
                username="alice",
                password_hash=hashed,
            ),
            return_new=False,
        )

    authn = _orchestrator(
        eligibility=_eligibility(ctx),
        password_svc=pwd_svc,
        pa_qry=ctx.document.query(password_account_spec),
        methods=frozenset({"password"}),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        identity = await authn.authenticate_with_password(
            PasswordCredentials(login="alice", password="correct horse battery staple"),
        )
    assert identity.identity.principal_id == pid

    with pytest.raises(Exception, match="Invalid password|authentication"):
        with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            await authn.authenticate_with_password(
                PasswordCredentials(login="alice", password="wrong"),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_issue_oauth_tokens_and_bearer_auth(pg_client: PostgresClient) -> None:
    """Persist a refresh session row, then authenticate the issued access JWT via the orchestrator."""

    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    access_svc = AccessTokenService(secret_key=secrets.token_bytes(32))
    refresh_svc = RefreshTokenService(
        pepper=pepper,
        config=RefreshTokenConfig(expires_in=timedelta(days=30)),
    )

    token_adapter = TokenLifecycleAdapter(
        access_svc=access_svc,
        refresh_svc=refresh_svc,
        session_qry=ctx.document.query(session_spec),
        session_cmd=ctx.document.command(session_spec),
        eligibility=_eligibility(ctx),
    )

    identity = AuthnIdentity(principal_id=pid)
    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await token_adapter.issue_tokens(identity)

    access_creds = issued.access.token
    assert issued.refresh is not None

    sub = AccessTokenCredentials(
        token=access_creds.token,
        scheme=access_creds.scheme,
    )

    authn = _orchestrator(
        eligibility=_eligibility(ctx),
        access_svc=access_svc,
        methods=frozenset({"token"}),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        bearer_id = await authn.authenticate_with_token(sub)

    assert bearer_id.identity.principal_id == pid

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        page = await ctx.document.query(session_spec).find_many(
            filters={"$values": {"principal_id": pid}}
        )

    assert len(page.hits) == 1
    assert page.hits[0].refresh_digest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_api_key_issue_and_authenticate(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    api_key_svc = ApiKeyService(pepper=pepper, config=ApiKeyConfig(prefix="sk_test"))
    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    ak_cmd = ctx.document.command(api_key_account_spec)

    lifecycle = ApiKeyLifecycleAdapter(
        api_key_svc=api_key_svc,
        ak_qry=ctx.document.query(api_key_account_spec),
        ak_cmd=ak_cmd,
        eligibility=_eligibility(ctx),
    )

    authn = _orchestrator(
        eligibility=_eligibility(ctx),
        api_key_svc=api_key_svc,
        ak_qry=ctx.document.query(api_key_account_spec),
        methods=frozenset({"api_key"}),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        resp = await lifecycle.issue_api_key(AuthnIdentity(principal_id=pid))

    issued_key = resp.key.key
    assert resp.key.prefix == "sk_test"

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        authed = await authn.authenticate_with_api_key(
            ApiKeyCredentials(key=issued_key, prefix=resp.key.prefix),
        )
    assert authed.identity.principal_id == pid


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_change_password(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pwd_svc = PasswordService()
    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    pwd_cmd = ctx.document.command(password_account_spec)
    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await pwd_cmd.create(
            CreatePasswordAccountCmd(
                principal_id=pid,
                username="bob",
                password_hash=pwd_svc.hash_password("old-secret"),
            ),
            return_new=False,
        )

    plc = PasswordLifecycleAdapter(
        password_svc=pwd_svc,
        pa_qry=ctx.document.query(password_account_spec),
        pa_cmd=pwd_cmd,
        eligibility=_eligibility(ctx),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await plc.change_password(AuthnIdentity(principal_id=pid), "new-secret")

    authn = _orchestrator(
        eligibility=_eligibility(ctx),
        password_svc=pwd_svc,
        pa_qry=ctx.document.query(password_account_spec),
        methods=frozenset({"password"}),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await authn.authenticate_with_password(
            PasswordCredentials(login="bob", password="new-secret"),
        )

    with pytest.raises(Exception, match="Invalid password|authentication"):
        with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            await authn.authenticate_with_password(
                PasswordCredentials(login="bob", password="old-secret"),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_provision_password_account(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    provisioning = PasswordAccountProvisioningAdapter(
        password_svc=PasswordService(),
        password_account_qry=ctx.document.query(password_account_spec),
        password_account_cmd=ctx.document.command(password_account_spec),
        eligibility=_eligibility(ctx),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await provisioning.register_with_password(
            pid,
            PasswordCredentials(login="carol", password="initial"),
        )

    pwd_qry = ctx.document.query(password_account_spec)
    authn = _orchestrator(
        eligibility=_eligibility(ctx),
        password_svc=PasswordService(),
        pa_qry=pwd_qry,
        methods=frozenset({"password"}),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await authn.authenticate_with_password(
            PasswordCredentials(login="carol", password="initial"),
        )


def _integration_password_config() -> PasswordConfig:
    return PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_execution_deps_password_authentication(
    pg_client: PostgresClient,
) -> None:
    """Resolve the orchestrator via :class:`AuthnDepsModule` merged with Postgres document deps."""

    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    base_ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    kernel = AuthnKernelConfig(
        password=_integration_password_config(),
        api_key_pepper=pepper,
    )

    authn_part = AuthnDepsModule(
        kernel=kernel,
        authn={"default": frozenset({"password", "api_key"})},
    )()

    ctx = context_from_deps(
        _authn_pg_deps(pg_client, suffix=suffix).merge(authn_part),
    )

    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    shared = build_authn_shared_services(kernel)
    pwd_svc = shared.password_svc
    assert pwd_svc is not None

    hashed = pwd_svc.hash_password("correct horse battery staple")
    pwd_cmd = ctx.document.command(password_account_spec)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await pwd_cmd.create(
            CreatePasswordAccountCmd(
                principal_id=pid,
                username="alice",
                password_hash=hashed,
            ),
            return_new=False,
        )

    spec = AuthnSpec(name="default", enabled_methods=frozenset({"password", "api_key"}))
    factory = ctx.deps.provide(AuthnDepKey, route="default")
    authn = factory(ctx, spec)

    assert isinstance(authn, AuthnOrchestrator)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        identity = await authn.authenticate_with_password(
            PasswordCredentials(login="alice", password="correct horse battery staple"),
        )

    assert identity.identity.principal_id == pid


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_execution_deps_issue_tokens_and_bearer_auth(
    pg_client: PostgresClient,
) -> None:
    """Token lifecycle and bearer auth using shared kernel-backed :class:`AuthnDepsModule`."""

    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    access_secret = secrets.token_bytes(32)
    base_ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    kernel = AuthnKernelConfig(
        access_token_secret=access_secret,
        refresh_token_pepper=pepper,
        refresh_token=RefreshTokenConfig(expires_in=timedelta(days=30)),
        password=_integration_password_config(),
        api_key_pepper=pepper,
    )

    methods = frozenset({"token", "password", "api_key"})

    exec_deps = AuthnDepsModule(
        kernel=kernel,
        authn={"oauth": methods},
        token_lifecycle={"oauth"},
    )()

    ctx = context_from_deps(
        _authn_pg_deps(pg_client, suffix=suffix).merge(exec_deps),
    )

    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    spec = AuthnSpec(name="oauth", enabled_methods=methods)

    tl_factory = ctx.deps.provide(TokenLifecycleDepKey, route="oauth")
    token_adapter = tl_factory(ctx, spec)
    assert isinstance(token_adapter, TokenLifecycleAdapter)

    identity = AuthnIdentity(principal_id=pid)
    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await token_adapter.issue_tokens(identity)

    access_creds = issued.access.token
    assert issued.refresh is not None

    sub = AccessTokenCredentials(
        token=access_creds.token,
        scheme=access_creds.scheme,
    )

    auth_factory = ctx.deps.provide(AuthnDepKey, route="oauth")
    authn = auth_factory(ctx, spec)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        bearer_id = await authn.authenticate_with_token(sub)

    assert bearer_id.identity.principal_id == pid

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        page = await ctx.document.query(session_spec).find_many(
            filters={"$values": {"principal_id": pid}}
        )

    assert len(page.hits) == 1
    assert page.hits[0].refresh_digest
