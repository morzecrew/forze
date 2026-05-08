"""Integration tests for authn adapters against Postgres document gateways."""

from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import UUID, uuid4

import pytest

pytest.importorskip("argon2")
pytest.importorskip("jwt")

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnIdentity,
    PasswordCredentials,
    TokenCredentials,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.context import CallContext
from forze_authnz.authn.adapters import (
    ApiKeyLifecycleAdapter,
    AuthnAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    TokenLifecycleAdapter,
)
from forze_authnz.authn.application.constants import AuthnResourceName
from forze_authnz.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    principal_spec,
    session_spec,
)
from forze_authnz.authn.domain.models.account import (
    CreateApiKeyAccountCmd,
    CreatePasswordAccountCmd,
)
from forze_authnz.authn.services import (
    AccessTokenService,
    ApiKeyConfig,
    ApiKeyService,
    PasswordService,
    RefreshTokenConfig,
    RefreshTokenService,
)
from forze_postgres.execution.deps.deps import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient

# ----------------------- #


async def _authn_pg_setup(
    pg_client: PostgresClient,
    *,
    suffix: str,
) -> ExecutionContext:
    """Create authn DDL and routed Postgres document adapters."""

    pri = f"authn_pri_{suffix}"
    pwd = f"authn_pwd_{suffix}"
    ak = f"authn_ak_{suffix}"
    sess = f"authn_sess_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {pri} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            is_active boolean NOT NULL
        );
        CREATE TABLE {pwd} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            username text NOT NULL,
            email text,
            password_hash text NOT NULL,
            is_active boolean NOT NULL DEFAULT true
        );
        CREATE TABLE {ak} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            prefix text,
            key_hash text NOT NULL,
            is_active boolean NOT NULL DEFAULT true
        );
        CREATE TABLE {sess} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            family_id uuid NOT NULL,
            refresh_digest bytea NOT NULL,
            expires_at timestamptz NOT NULL,
            revoked_at timestamptz,
            rotated_at timestamptz,
            replaced_by uuid
        );
        """
    )

    ro = {"read": ("public", pri)}
    pwd_cfg = {
        "read": ("public", pwd),
        "write": ("public", pwd),
        "bookkeeping_strategy": "application",
    }
    ak_cfg = {
        "read": ("public", ak),
        "write": ("public", ak),
        "bookkeeping_strategy": "application",
    }
    sess_cfg = {
        "read": ("public", sess),
        "write": ("public", sess),
        "bookkeeping_strategy": "application",
    }

    introspector = PostgresIntrospector(client=pg_client)

    query_routes = {
        AuthnResourceName.PRINCIPALS: ConfigurablePostgresReadOnlyDocument(config=ro),
        AuthnResourceName.PASSWORD_ACCOUNTS: ConfigurablePostgresDocument(
            config=pwd_cfg
        ),
        AuthnResourceName.API_KEY_ACCOUNTS: ConfigurablePostgresDocument(config=ak_cfg),
        AuthnResourceName.TOKEN_SESSIONS: ConfigurablePostgresDocument(config=sess_cfg),
    }

    cmd_routes = {
        AuthnResourceName.PASSWORD_ACCOUNTS: ConfigurablePostgresDocument(
            config=pwd_cfg
        ),
        AuthnResourceName.API_KEY_ACCOUNTS: ConfigurablePostgresDocument(config=ak_cfg),
        AuthnResourceName.TOKEN_SESSIONS: ConfigurablePostgresDocument(config=sess_cfg),
    }

    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: introspector,
            }
        ).merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: query_routes,
                    DocumentCommandDepKey: cmd_routes,
                }
            ),
        ),
    )


async def _insert_principal_row(
    pg_client: PostgresClient,
    *,
    table: str,
    principal_id: UUID,
) -> None:
    """Insert one ``ReadPrincipal`` row for adapter validation."""

    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, rev, created_at, last_update_at, is_active)
        VALUES (%s, 1, now(), now(), true)
        """,
        (principal_id,),
    )


def _call_ctx() -> CallContext:
    return CallContext(execution_id=uuid4(), correlation_id=uuid4())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_password_authentication(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pwd_svc = PasswordService()
    pid = uuid4()
    await _insert_principal_row(
        pg_client, table=f"authn_pri_{suffix}", principal_id=pid
    )

    hashed = pwd_svc.hash_password("correct horse battery staple")
    pwd_cmd = ctx.doc_command(password_account_spec)

    with ctx.bind_call(call=_call_ctx()):
        await pwd_cmd.create(
            CreatePasswordAccountCmd(
                principal_id=pid,
                username="alice",
                password_hash=hashed,
            ),
            return_new=False,
        )

    authn = AuthnAdapter(
        access_svc=AccessTokenService(secret_key=secrets.token_bytes(32)),
        password_svc=pwd_svc,
        pa_qry=ctx.doc_query(password_account_spec),
        api_key_svc=ApiKeyService(pepper=pepper),
        ak_qry=ctx.doc_query(api_key_account_spec),
    )

    with ctx.bind_call(call=_call_ctx()):
        identity = await authn.authenticate_with_password(
            PasswordCredentials(login="alice", password="correct horse battery staple"),
        )
    assert identity.principal_id == pid

    with pytest.raises(Exception, match="Invalid password|authentication"):
        with ctx.bind_call(call=_call_ctx()):
            await authn.authenticate_with_password(
                PasswordCredentials(login="alice", password="wrong"),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_issue_oauth_tokens_and_bearer_auth(pg_client: PostgresClient) -> None:
    """Persist a refresh session row, then authenticate the issued access JWT via :class:`AuthnAdapter`."""

    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pid = uuid4()
    await _insert_principal_row(
        pg_client, table=f"authn_pri_{suffix}", principal_id=pid
    )

    access_svc = AccessTokenService(secret_key=secrets.token_bytes(32))
    refresh_svc = RefreshTokenService(
        pepper=pepper,
        config=RefreshTokenConfig(expires_in=timedelta(days=30)),
    )

    token_adapter = TokenLifecycleAdapter(
        access_svc=access_svc,
        refresh_svc=refresh_svc,
        session_qry=ctx.doc_query(session_spec),
        session_cmd=ctx.doc_command(session_spec),
        principal_qry=ctx.doc_query(principal_spec),
    )

    identity = AuthnIdentity(principal_id=pid)
    with ctx.bind_call(call=_call_ctx()):
        issued = await token_adapter.issue_tokens(identity)

    access_creds = issued.access_token.token
    assert issued.refresh_token is not None

    sub = TokenCredentials(
        token=access_creds.token,
        scheme=access_creds.scheme,
        kind=access_creds.kind,
    )

    authn = AuthnAdapter(
        access_svc=access_svc,
        password_svc=PasswordService(),
        pa_qry=ctx.doc_query(password_account_spec),
        api_key_svc=ApiKeyService(pepper=pepper),
        ak_qry=ctx.doc_query(api_key_account_spec),
    )

    with ctx.bind_call(call=_call_ctx()):
        bearer_id = await authn.authenticate_with_token(sub)

    assert bearer_id.principal_id == pid

    with ctx.bind_call(call=_call_ctx()):
        page = await ctx.doc_query(session_spec).find_many(
            filters={"$fields": {"principal_id": pid}}
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
    await _insert_principal_row(
        pg_client, table=f"authn_pri_{suffix}", principal_id=pid
    )

    bootstrap_key_material = secrets.token_hex(16)
    bootstrap_digest = api_key_svc.calculate_key_digest(bootstrap_key_material)

    ak_cmd = ctx.doc_command(api_key_account_spec)

    with ctx.bind_call(call=_call_ctx()):
        await ak_cmd.create(
            CreateApiKeyAccountCmd(
                principal_id=pid,
                key_hash=bootstrap_digest,
                prefix=api_key_svc.config.prefix,
            ),
            return_new=False,
        )

    pwd_svc = PasswordService()

    lifecycle = ApiKeyLifecycleAdapter(
        api_key_svc=api_key_svc,
        ak_qry=ctx.doc_query(api_key_account_spec),
        ak_cmd=ak_cmd,
        principal_qry=ctx.doc_query(principal_spec),
    )

    authn = AuthnAdapter(
        access_svc=AccessTokenService(secret_key=secrets.token_bytes(32)),
        password_svc=pwd_svc,
        pa_qry=ctx.doc_query(password_account_spec),
        api_key_svc=api_key_svc,
        ak_qry=ctx.doc_query(api_key_account_spec),
    )

    with ctx.bind_call(call=_call_ctx()):
        resp = await lifecycle.issue_api_key(AuthnIdentity(principal_id=pid))

    issued_key = resp.key.key
    assert resp.key.prefix == "sk_test"

    with ctx.bind_call(call=_call_ctx()):
        authed = await authn.authenticate_with_api_key(
            ApiKeyCredentials(key=issued_key, prefix=resp.key.prefix),
        )
    assert authed.principal_id == pid


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_change_password(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pwd_svc = PasswordService()
    pid = uuid4()
    await _insert_principal_row(
        pg_client, table=f"authn_pri_{suffix}", principal_id=pid
    )

    pwd_cmd = ctx.doc_command(password_account_spec)
    with ctx.bind_call(call=_call_ctx()):
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
        pa_qry=ctx.doc_query(password_account_spec),
        pa_cmd=pwd_cmd,
    )

    with ctx.bind_call(call=_call_ctx()):
        await plc.change_password(AuthnIdentity(principal_id=pid), "new-secret")

    authn = AuthnAdapter(
        access_svc=AccessTokenService(secret_key=secrets.token_bytes(32)),
        password_svc=pwd_svc,
        pa_qry=ctx.doc_query(password_account_spec),
        api_key_svc=ApiKeyService(pepper=pepper),
        ak_qry=ctx.doc_query(api_key_account_spec),
    )

    with ctx.bind_call(call=_call_ctx()):
        await authn.authenticate_with_password(
            PasswordCredentials(login="bob", password="new-secret"),
        )

    with pytest.raises(Exception, match="Invalid password|authentication"):
        with ctx.bind_call(call=_call_ctx()):
            await authn.authenticate_with_password(
                PasswordCredentials(login="bob", password="old-secret"),
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pg_provision_password_account(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)

    pid = uuid4()
    await _insert_principal_row(
        pg_client, table=f"authn_pri_{suffix}", principal_id=pid
    )

    provisioning = PasswordAccountProvisioningAdapter(
        password_svc=PasswordService(),
        password_account_qry=ctx.doc_query(password_account_spec),
        password_account_cmd=ctx.doc_command(password_account_spec),
        principal_qry=ctx.doc_query(principal_spec),
    )

    with ctx.bind_call(call=_call_ctx()):
        await provisioning.register_with_password(
            pid,
            PasswordCredentials(login="carol", password="initial"),
        )

    pwd_qry = ctx.doc_query(password_account_spec)
    authn = AuthnAdapter(
        access_svc=AccessTokenService(secret_key=secrets.token_bytes(32)),
        password_svc=PasswordService(),
        pa_qry=pwd_qry,
        api_key_svc=ApiKeyService(pepper=secrets.token_bytes(32)),
        ak_qry=ctx.doc_query(api_key_account_spec),
    )

    with ctx.bind_call(call=_call_ctx()):
        await authn.authenticate_with_password(
            PasswordCredentials(login="carol", password="initial"),
        )
