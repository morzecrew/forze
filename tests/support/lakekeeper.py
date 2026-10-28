"""A Lakekeeper catalog that vends per-tenant credentials, for the RFC 0078 lake-read probes.

RFC 0018 P3's vended-credential fixture does not exist yet; this is the minimal stand-in RFC 0078
P0 needed. It runs, on one Docker network:

* **Lakekeeper** (the Iceberg REST catalog) on its own Postgres,
* **OpenFGA** as the authorizer — a real one, because ``AllowAll`` records grants and enforces
  none (RFC 0078 §3.1), so a fixture running it would prove nothing,
* **Keycloak** as the OAuth2 client-credentials IdP — one confidential client per principal,
* **RustFS** as the object store. It serves STS ``AssumeRole`` and honours the session policy
  Lakekeeper downscopes with, which the fixture's own self-test checks rather than assumes.

Two tenants each get one warehouse (``sts-enabled: true``, ``remote-signing-enabled: false``)
and one principal holding ``select`` on that warehouse and nothing else. A third principal holds
no grant at all. Each warehouse carries one small Iceberg table, ``gold.orders``, whose rows name
their tenant.

**Addressing.** The store's endpoint and the IdP's issuer are each one string that both the
containers and the test process must reach: Lakekeeper hands the storage-profile endpoint to the
client verbatim, and validates the token issuer against the URL it discovered. Both are
therefore addressed through the network's gateway IP and the host-published port, which works
on a Linux Docker host (and on CI runners), not on Docker Desktop. The stack probes the gateway
once and skips when the host cannot reach it.

**Exposure.** Every secret here is generated per run and lives only in this process and the
containers' environment: the store's root key, Keycloak's admin password, the client secrets
(the realm file names them as ``${ENV}`` placeholders), Postgres's password. RustFS, Keycloak
and Lakekeeper publish their ports on all host interfaces, as testcontainers does by default,
so for the module's lifetime anyone on the host's network can reach their unauthenticated
endpoints. OpenFGA (no authn) and Postgres publish nothing. Do not run this on a host whose
network you do not trust.
"""

from __future__ import annotations

import base64
import json
import secrets
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import docker
import duckdb
import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from tests.support.rustfs import RustfsContainer

LAKEKEEPER_IMAGE = "quay.io/lakekeeper/catalog:v0.14.2"
OPENFGA_IMAGE = "openfga/openfga:v1.22.0"
KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak:26.8.1"
POSTGRES_IMAGE = "ghcr.io/morzecrew/postgres:18"

REALM = "iceberg"
AUDIENCE = "lakekeeper"
OAUTH2_SCOPE = "profile"
BUCKET = "lake"
TABLE = ("gold", "orders")
ROOT_ACCESS_KEY = "lakeroot"

_ADMIN = "lake-admin"
_TENANTS = ("tenant-a", "tenant-b")
_UNGRANTED = "tenant-c"

# ----------------------- #


@dataclass(frozen=True, slots=True)
class LakePrincipal:
    """One OAuth2 client-credentials principal, as Lakekeeper knows it (``oidc~<sub>``)."""

    client_id: str
    client_secret: str
    user_id: str


@dataclass(frozen=True, slots=True)
class LakeTenant:
    """A tenant: its principal, its warehouse, and the rows its one table holds."""

    name: str
    principal: LakePrincipal
    warehouse: str
    warehouse_id: str
    key_prefix: str
    rows: tuple[tuple[int, str], ...]


@dataclass(slots=True)
class LakeVendingStack:
    """Addresses and identities of a running stack; see the module docstring."""

    catalog_uri: str
    management_uri: str
    token_uri: str
    s3_endpoint: str
    root_access_key: str
    root_secret_key: str
    admin: LakePrincipal
    tenants: dict[str, LakeTenant]
    ungranted: LakePrincipal
    lakekeeper: DockerContainer

    def token(self, principal: LakePrincipal) -> str:
        """A fresh access token for ``principal``."""

        return fetch_token(self.token_uri, principal.client_id, principal.client_secret)

    def admin_client(self) -> httpx.Client:
        """A management-API client acting as the bootstrapped operator."""

        return httpx.Client(
            base_url=self.management_uri,
            headers={"Authorization": f"Bearer {self.token(self.admin)}"},
            timeout=30,
        )

    def admin_catalog(self, tenant: LakeTenant) -> Any:
        """A PyIceberg REST catalog on ``tenant``'s warehouse, acting as the operator."""

        from pyiceberg.catalog.rest import RestCatalog

        return RestCatalog(
            "admin",
            uri=self.catalog_uri,
            warehouse=tenant.warehouse,
            credential=f"{self.admin.client_id}:{self.admin.client_secret}",
            **{"oauth2-server-uri": self.token_uri, "scope": OAUTH2_SCOPE},
        )

    def set_grant(
        self,
        tenant: LakeTenant,
        principal: LakePrincipal,
        *,
        granted: bool,
        privilege: str = "select",
    ) -> None:
        """Grant or revoke ``privilege`` for ``principal`` on ``tenant``'s warehouse."""

        entry = {"privilege": privilege, "principal": {"user": principal.user_id}}
        body = {"writes": [entry]} if granted else {"deletes": [entry]}
        with self.admin_client() as c:
            r = c.post(f"/warehouse/{tenant.warehouse_id}/grants", json=body)
            r.raise_for_status()

    def load_table(self, principal: LakePrincipal, warehouse: str) -> httpx.Response:
        """``LoadTable`` for ``gold.orders`` with vended credentials, straight over REST."""

        headers = {"Authorization": f"Bearer {self.token(principal)}"}
        cfg = httpx.get(
            f"{self.catalog_uri}/v1/config", params={"warehouse": warehouse}, headers=headers
        )
        if cfg.status_code != 200:
            return cfg
        prefix = cfg.json()["defaults"]["prefix"]
        return httpx.get(
            f"{self.catalog_uri}/v1/{prefix}/namespaces/{TABLE[0]}/tables/{TABLE[1]}",
            headers={**headers, "X-Iceberg-Access-Delegation": "vended-credentials"},
        )

    def session(
        self,
        principal: LakePrincipal,
        warehouse: str,
        *,
        before_attach: tuple[str, ...] = (),
        attach_options: str = "",
        global_s3_off: bool = True,
    ) -> Any:
        """A ``:memory:`` DuckDB attached to ``warehouse`` as ``principal`` — RFC 0078 §3.4 steps 1-4.

        Steps 5-8 are left to the caller, so a probe can place them where it needs them.
        """

        con = duckdb.connect(
            ":memory:",
            config={
                "allow_persistent_secrets": False,
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
            },
        )
        con.execute("LOAD httpfs")
        con.execute("LOAD iceberg")
        if global_s3_off:
            con.execute("SET enable_global_s3_configuration=false")
            for name in ("s3_access_key_id", "s3_secret_access_key", "s3_session_token"):
                con.execute(f"SET {name}=''")
        for statement in before_attach:
            con.execute(statement)
        con.execute(
            f"CREATE SECRET lake_cat (TYPE iceberg, CLIENT_ID '{principal.client_id}', "
            f"CLIENT_SECRET '{principal.client_secret}', OAUTH2_SERVER_URI '{self.token_uri}', "
            f"OAUTH2_SCOPE '{OAUTH2_SCOPE}')"
        )
        con.execute(
            f"ATTACH '{warehouse}' AS lake (TYPE iceberg, ENDPOINT '{self.catalog_uri}', "
            f"SECRET lake_cat, ACCESS_DELEGATION_MODE 'vended_credentials'{attach_options})"
        )
        return con


# ----------------------- #


def fetch_token(token_uri: str, client_id: str, client_secret: str) -> str:
    """Run the client-credentials grant and return the access token."""

    r = httpx.post(
        token_uri,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": OAUTH2_SCOPE,
        },
        timeout=30,
    )
    r.raise_for_status()
    return str(r.json()["access_token"])


def _jwt_claims(token: str) -> dict[str, Any]:
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def _wait_http(url: str, *, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"{url} did not answer 200 within {timeout}s: {last!r}")


def _realm_env_var(client_id: str) -> str:
    return "LAKE_CLIENT_SECRET_" + client_id.upper().replace("-", "_")


def _realm(
    client_ids: Iterable[str], *, tenant_token_lifespan: int | None = None
) -> dict[str, Any]:
    """A realm with one confidential service-account client per principal.

    Each secret is a placeholder Keycloak resolves from its own environment at import, so the
    file on disk holds none. ``tenant_token_lifespan`` overrides the access-token lifespan of
    the tenant principals only, so the operator's seeding never meets an expired token.
    """

    return {
        "realm": REALM,
        "enabled": True,
        "accessTokenLifespan": 600,
        "clients": [
            {
                "clientId": client_id,
                "secret": f"${{{_realm_env_var(client_id)}}}",
                "enabled": True,
                "publicClient": False,
                "clientAuthenticatorType": "client-secret",
                "serviceAccountsEnabled": True,
                "standardFlowEnabled": False,
                "directAccessGrantsEnabled": False,
                "attributes": (
                    {"access.token.lifespan": str(tenant_token_lifespan)}
                    if tenant_token_lifespan is not None and client_id in _TENANTS
                    else {}
                ),
                "protocolMappers": [
                    {
                        "name": "lakekeeper-audience",
                        "protocol": "openid-connect",
                        "protocolMapper": "oidc-audience-mapper",
                        "config": {
                            "included.custom.audience": AUDIENCE,
                            "access.token.claim": "true",
                            "id.token.claim": "false",
                        },
                    }
                ],
            }
            for client_id in client_ids
        ],
    }


def _network_gateway(network: Network) -> str:
    client = docker.from_env()
    try:
        return str(client.networks.get(network.name).attrs["IPAM"]["Config"][0]["Gateway"])
    finally:
        client.close()


def _run_to_completion(container: DockerContainer, *, timeout: float = 120.0) -> None:
    container.start()
    wrapped = container.get_wrapped_container()
    result = wrapped.wait(timeout=timeout)
    if result.get("StatusCode") != 0:
        logs = wrapped.logs().decode(errors="replace")[-4000:]
        raise RuntimeError(f"{container.image} exited {result}: {logs}")


def _seed_table(stack: LakeVendingStack, tenant: LakeTenant) -> None:
    """Create ``gold.orders`` in the tenant's warehouse as the operator, through the REST catalog."""

    import pyarrow as pa

    catalog = stack.admin_catalog(tenant)
    catalog.create_namespace_if_not_exists(TABLE[0])
    data = pa.table(
        {
            "id": pa.array([r[0] for r in tenant.rows], pa.int64()),
            "tenant": pa.array([r[1] for r in tenant.rows], pa.string()),
        }
    )
    table = catalog.create_table(TABLE, schema=data.schema)
    table.append(data)


def _install_extensions() -> None:
    """Install ``httpfs``, ``iceberg`` and ``avro`` into the default extension directory.

    Sessions load them with autoinstall off (RFC 0078 §3.4), so a cold runner needs this once.
    ``iceberg`` loads ``avro`` itself and fails without it (EXECUTION-LOG D-071).
    """

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL httpfs")
        con.execute("INSTALL iceberg")
        con.execute("INSTALL avro")
    finally:
        con.close()


def _skip_unless_gateway_reachable(url: str) -> None:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if resp.status == 200:
                return
    except (urllib.error.URLError, TimeoutError, OSError):
        pass
    pytest.skip(f"the Docker network gateway is not reachable from this host ({url})")


# ----------------------- #


@contextmanager
def lake_vending_stack(
    *,
    tenant_token_lifespan: int | None = None,
    sts_token_validity_seconds: int | None = None,
) -> Iterator[LakeVendingStack]:
    """Start the whole stack, bootstrap it, seed both tenants, and yield its addresses.

    :param tenant_token_lifespan: Access-token lifespan of the tenant principals, in seconds;
        ``None`` keeps the realm's 600.
    :param sts_token_validity_seconds: The warehouses' ``sts-token-validity-seconds``; ``None``
        keeps Lakekeeper's default.
    """

    client_ids = (_ADMIN, *_TENANTS, _UNGRANTED)
    client_secrets = {name: secrets.token_hex(16) for name in client_ids}
    root_secret_key, pg_password = secrets.token_hex(16), secrets.token_hex(16)
    _install_extensions()

    with ExitStack() as stack, tempfile.TemporaryDirectory() as realm_dir:
        realm = _realm(client_ids, tenant_token_lifespan=tenant_token_lifespan)
        Path(realm_dir, "realm.json").write_text(json.dumps(realm))
        Path(realm_dir).chmod(0o755)
        Path(realm_dir, "realm.json").chmod(0o644)

        network = stack.enter_context(Network())
        gateway = _network_gateway(network)

        s3 = stack.enter_context(
            RustfsContainer(access_key=ROOT_ACCESS_KEY, secret_key=root_secret_key)
            .with_network(network)
            .with_network_aliases("s3")
        )
        s3_port = s3.get_exposed_port(9000)
        _wait_http(f"http://{s3.get_container_host_ip()}:{s3_port}/minio/health/live")
        s3_endpoint = f"http://{gateway}:{s3_port}"
        _skip_unless_gateway_reachable(f"{s3_endpoint}/minio/health/live")
        s3.get_client().make_bucket(BUCKET)

        keycloak = (
            DockerContainer(KEYCLOAK_IMAGE)
            .with_network(network)
            .with_exposed_ports(8080)
            .with_env("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .with_env("KC_BOOTSTRAP_ADMIN_PASSWORD", secrets.token_hex(16))
            .with_volume_mapping(realm_dir, "/opt/keycloak/data/import", "ro")
            .with_command("start-dev --import-realm")
        )
        for client_id, secret in client_secrets.items():
            keycloak.with_env(_realm_env_var(client_id), secret)
        stack.enter_context(keycloak)
        issuer = f"http://{gateway}:{keycloak.get_exposed_port(8080)}/realms/{REALM}"
        _wait_http(f"{issuer}/.well-known/openid-configuration", timeout=180)
        token_uri = f"{issuer}/protocol/openid-connect/token"

        stack.enter_context(
            DockerContainer(POSTGRES_IMAGE)
            .with_network(network)
            .with_network_aliases("lk-db")
            .with_env("POSTGRES_PASSWORD", pg_password)
        )
        # Publishes no port; the migrate loop below waits for it. The check caches are pinned
        # off, so a revoked grant is refused on the next check (EXECUTION-LOG D-032).
        stack.enter_context(
            DockerContainer(OPENFGA_IMAGE)
            .with_network(network)
            .with_network_aliases("openfga")
            .with_env("OPENFGA_DATASTORE_ENGINE", "memory")
            .with_env("OPENFGA_PLAYGROUND_ENABLED", "false")
            .with_env("OPENFGA_CHECK_QUERY_CACHE_ENABLED", "false")
            .with_env("OPENFGA_CHECK_ITERATOR_CACHE_ENABLED", "false")
            .with_env("OPENFGA_CACHE_CONTROLLER_ENABLED", "false")
            .with_command("run")
        )

        lk_env = {
            "LAKEKEEPER__PG_DATABASE_URL_WRITE": (
                f"postgresql://postgres:{pg_password}@lk-db:5432/postgres"
            ),
            "LAKEKEEPER__PG_ENCRYPTION_KEY": secrets.token_hex(16),
            "LAKEKEEPER__AUTHZ_BACKEND": "openfga",
            "LAKEKEEPER__OPENFGA__ENDPOINT": "http://openfga:8081",
            "LAKEKEEPER__OPENID_PROVIDER_URI": issuer,
            "LAKEKEEPER__OPENID_AUDIENCE": AUDIENCE,
            "LAKEKEEPER__OPENID_SUBJECT_CLAIM": "sub",
        }
        migrate = DockerContainer(LAKEKEEPER_IMAGE).with_network(network).with_command("migrate")
        for k, v in lk_env.items():
            migrate.with_env(k, v)
        stack.callback(migrate.stop)
        deadline = time.monotonic() + 60
        while True:  # Postgres and OpenFGA take a moment to come up; migrate does not retry.
            try:
                _run_to_completion(migrate)
                break
            except RuntimeError:
                if time.monotonic() > deadline:
                    raise
                migrate.stop()
                time.sleep(1)

        lakekeeper = (
            DockerContainer(LAKEKEEPER_IMAGE).with_network(network).with_exposed_ports(8181)
        )
        for k, v in lk_env.items():
            lakekeeper.with_env(k, v)
        stack.enter_context(lakekeeper.with_command("serve"))
        base = f"http://{lakekeeper.get_container_host_ip()}:{lakekeeper.get_exposed_port(8181)}"
        _wait_http(f"{base}/health")

        def principal(client_id: str) -> LakePrincipal:
            sub = _jwt_claims(fetch_token(token_uri, client_id, client_secrets[client_id]))["sub"]
            return LakePrincipal(client_id, client_secrets[client_id], f"oidc~{sub}")

        admin = principal(_ADMIN)
        out = LakeVendingStack(
            catalog_uri=f"{base}/catalog",
            management_uri=f"{base}/management/v1",
            token_uri=token_uri,
            s3_endpoint=s3_endpoint,
            root_access_key=ROOT_ACCESS_KEY,
            root_secret_key=root_secret_key,
            admin=admin,
            tenants={},
            ungranted=principal(_UNGRANTED),
            lakekeeper=lakekeeper,
        )

        with out.admin_client() as c:
            c.post(
                "/bootstrap", json={"accept-terms-of-use": True, "is-operator": True}
            ).raise_for_status()
            for name in (*_TENANTS, _UNGRANTED):
                p = out.ungranted if name == _UNGRANTED else principal(name)
                c.post(
                    "/user",
                    json={
                        "id": p.user_id,
                        "name": name,
                        "user-type": "application",
                        "update-if-exists": True,
                    },
                ).raise_for_status()
                if name == _UNGRANTED:
                    continue
                warehouse = f"wh-{name}"
                r = c.post(
                    "/warehouse",
                    json={
                        "warehouse-name": warehouse,
                        "storage-profile": {
                            "type": "s3",
                            "bucket": BUCKET,
                            "key-prefix": name,
                            "region": "local-01",
                            "endpoint": s3_endpoint,
                            "path-style-access": True,
                            "flavor": "s3-compat",
                            "sts-enabled": True,
                            "remote-signing-enabled": False,
                            **(
                                {"sts-token-validity-seconds": sts_token_validity_seconds}
                                if sts_token_validity_seconds is not None
                                else {}
                            ),
                        },
                        "storage-credential": {
                            "type": "s3",
                            "credential-type": "access-key",
                            "aws-access-key-id": ROOT_ACCESS_KEY,
                            "aws-secret-access-key": root_secret_key,
                        },
                        "delete-profile": {"type": "hard"},
                    },
                )
                r.raise_for_status()
                out.tenants[name] = LakeTenant(
                    name=name,
                    principal=p,
                    warehouse=warehouse,
                    warehouse_id=str(r.json()["warehouse-id"]),
                    key_prefix=name,
                    rows=tuple((i, name) for i in range(1, 4)),
                )

        for tenant in out.tenants.values():
            _seed_table(out, tenant)
            out.set_grant(tenant, tenant.principal, granted=True)

        yield out
