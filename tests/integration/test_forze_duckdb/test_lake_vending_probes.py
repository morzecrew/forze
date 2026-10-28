"""RFC 0078 P0: pickup probes for routed DuckDB lake reads, pinned against a real vending stack.

Every test here asserts a fact P0 observed on ``duckdb==1.5.6`` with the ``iceberg`` build the
core repository serves for it (``890b78a9c``) and ``httpfs`` ``4bc690d``, against Lakekeeper
v0.14.2 under OpenFGA with RustFS as the STS store (``tests/support/lakekeeper.py``), and
re-probed on ``duckdb==2.0.0`` (``iceberg`` ``4c0c0874c4``). Where the two builds differ, the
test pins each. They pin behaviour, including behaviour the RFC did not expect, so that an
engine or catalog bump which changes any of it fails here first. The findings themselves are
recorded in ``rfcs/EXECUTION-LOG.md`` (units "RFC 0078 P0 — pickup probes", "P2 —
tightening" and "DuckDB 2.0 adoption").

The three P0 gate items are battery 1 (``TestFixtureSelfTest``), battery 5
(``TestCrossTenantByPath``) and battery 11a (``TestRevocation``).
"""

from __future__ import annotations

import datetime
import http.server
import ipaddress
import os
import re
import socket
import ssl
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import boto3
import botocore
import duckdb
import httpx
import pyarrow as pa
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from forze_duckdb import LakeTenantCatalog
from forze_duckdb.kernel.client import client as client_module
from forze_duckdb.kernel.client import routed_client
from tests.support.lakekeeper import (
    BUCKET,
    OAUTH2_SCOPE,
    LakePrincipal,
    LakeTenant,
    LakeVendingStack,
    lake_vending_stack,
)

# ----------------------- #


@pytest.fixture(scope="module")
def lake(docker_available: None) -> Iterator[LakeVendingStack]:
    with lake_vending_stack() as stack:
        yield stack


@pytest.fixture
def tenant_a(lake: LakeVendingStack) -> LakeTenant:
    return lake.tenants["tenant-a"]


@pytest.fixture(scope="module")
def tls_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[str, Path]]:
    """A local HTTPS server with a throwaway self-signed CA: the CSV's URL and the CA's path."""

    signer = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(signer.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(signer, hashes.SHA256())
    )
    tmp = tmp_path_factory.mktemp("tls")
    ca = tmp / "ca.pem"
    ca.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    chain = tmp / "server.pem"
    chain.write_bytes(
        ca.read_bytes()
        + signer.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    class _Csv(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "4")
            self.end_headers()
            self.wfile.write(b"a\n1\n")

        def do_HEAD(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", "4")
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Csv)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(chain)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        yield f"https://127.0.0.1:{server.server_address[1]}/x.csv", ca

    finally:
        server.shutdown()


@pytest.fixture
def tenant_b(lake: LakeVendingStack) -> LakeTenant:
    return lake.tenants["tenant-b"]


def _http_log(con: Any) -> list[dict[str, str]]:
    """Requests DuckDB's HTTP logger saw: url, status, and the SigV4 key id if signed."""

    out = []
    for (message,) in con.execute("SELECT message FROM duckdb_logs WHERE type = 'HTTP'").fetchall():
        url = re.search(r"'url': '([^']*)'", message)
        status = re.search(r"'status': ([A-Za-z_0-9]+)", message)
        key = re.search(r"Credential=([A-Za-z0-9]+)/", message)
        out.append(
            {
                "url": url.group(1) if url else "",
                "status": status.group(1) if status else "",
                "key": key.group(1) if key else "",
            }
        )
    return out


def _own_data_file(con: Any) -> str:
    return str(
        con.execute("SELECT file_path FROM iceberg_metadata(lake.gold.orders)").fetchone()[0]
    )


def _s3(lake: LakeVendingStack, **creds: str) -> Any:
    signature = creds.pop("signature", "s3v4")
    return boto3.client(
        "s3",
        endpoint_url=lake.s3_endpoint,
        region_name="local-01",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version=botocore.UNSIGNED if signature == "unsigned" else signature,
        ),
        **creds,
    )


def _refused(fn: Callable[[], object]) -> str:
    try:
        fn()
    except ClientError as exc:
        return str(exc.response["Error"]["Code"])
    return "ALLOWED"


def _shipped_allowlist(lake: LakeVendingStack, tenant: LakeTenant) -> tuple[str, ...]:
    """The routed client's own allowlist statements, to set before ``ATTACH`` (D-062)."""

    catalog = LakeTenantCatalog(
        uri=lake.catalog_uri,
        warehouse=tenant.warehouse,
        oauth2_server_uri=lake.token_uri,
        oauth2_scope=OAUTH2_SCOPE,
        storage_prefix=f"s3://{BUCKET}/{tenant.key_prefix}/",
    )
    directories, paths = routed_client._allowlist(catalog, None)
    return (
        f"SET allowed_directories={directories!r}",
        f"SET allowed_paths={paths!r}",
        "SET enable_external_access=false",
    )


# ----------------------- #

_PROBED = {"1.5.6": "890b78a9c", "2.0.0": "4c0c0874c4"}
"""The builds the probes ran on: ``duckdb`` version to its ``iceberg`` extension version."""

_ON_DUCKDB_2 = int(duckdb.__version__.split(".", 1)[0]) >= 2
_VENDED = "SELECT count(*) FROM duckdb_secrets() WHERE name LIKE '__internal_ic_%'"


def test_the_engine_is_the_build_the_probes_ran_on() -> None:
    """Every other test here pins a fact about these builds; a bump has to re-run the probes."""

    con = duckdb.connect(":memory:")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    (iceberg,) = con.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'"
    ).fetchone()

    rerun = "re-run the RFC 0078 probes and add an EXECUTION-LOG entry before re-pinning"
    assert duckdb.__version__ in _PROBED, rerun
    assert iceberg == _PROBED[duckdb.__version__], rerun


class TestFixtureSelfTest:
    """Battery 1 — the negative control. If any of this fails, every later pass is meaningless."""

    def test_the_store_refuses_anonymous_reads(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        location = lake.load_table(tenant_a.principal, tenant_a.warehouse).json()
        key = location["metadata-location"].removeprefix(f"s3://{BUCKET}/")
        root = _s3(
            lake, aws_access_key_id=lake.root_access_key, aws_secret_access_key=lake.root_secret_key
        )
        anonymous = _s3(lake, signature="unsigned")

        assert _refused(lambda: root.get_object(Bucket=BUCKET, Key=key)) == "ALLOWED"  # it exists
        assert _refused(lambda: anonymous.get_object(Bucket=BUCKET, Key=key)) == "AccessDenied"

    def test_the_catalog_refuses_anonymous_requests(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        r = httpx.get(f"{lake.catalog_uri}/v1/config", params={"warehouse": tenant_a.warehouse})

        assert r.status_code == 401

    def test_a_principal_without_a_grant_gets_no_load_table(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        # Lakekeeper hides the warehouse rather than refusing it: 404, never 403.
        r = lake.load_table(lake.ungranted, tenant_a.warehouse)

        assert r.status_code == 404
        assert r.json()["error"]["type"] == "NoSuchWarehouseException"

    def test_profiles_report_sts_on_and_remote_signing_off(self, lake: LakeVendingStack) -> None:
        with lake.admin_client() as c:
            for tenant in lake.tenants.values():
                profile = c.get(f"/warehouse/{tenant.warehouse_id}").json()["storage-profile"]

                assert profile["sts-enabled"] is True
                assert profile["remote-signing-enabled"] is False

    def test_a_vended_credential_reads_its_own_table_only_and_writes_nowhere(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, tenant_b: LakeTenant
    ) -> None:
        loaded = lake.load_table(tenant_a.principal, tenant_a.warehouse).json()
        (credential,) = loaded[
            "storage-credentials"
        ]  # one credential per table (battery 17 can't arise)
        cfg = credential["config"]
        vended = _s3(
            lake,
            aws_access_key_id=cfg["s3.access-key-id"],
            aws_secret_access_key=cfg["s3.secret-access-key"],
            aws_session_token=cfg["s3.session-token"],
        )
        root = _s3(
            lake, aws_access_key_id=lake.root_access_key, aws_secret_access_key=lake.root_secret_key
        )
        a_loc = loaded["metadata"]["location"].removeprefix(f"s3://{BUCKET}/")
        b_loc = lake.load_table(tenant_b.principal, tenant_b.warehouse).json()["metadata"][
            "location"
        ]
        b_loc = b_loc.removeprefix(f"s3://{BUCKET}/")
        keys = [o["Key"] for o in root.list_objects_v2(Bucket=BUCKET)["Contents"]]
        a_key = next(k for k in keys if k.startswith(f"{a_loc}/data/"))
        b_key = next(k for k in keys if k.startswith(f"{b_loc}/data/"))

        assert _refused(lambda: vended.get_object(Bucket=BUCKET, Key=a_key)) == "ALLOWED"
        assert _refused(lambda: vended.get_object(Bucket=BUCKET, Key=b_key)) == "AccessDenied"
        assert _refused(lambda: vended.list_objects_v2(Bucket=BUCKET, Prefix="")) == "AccessDenied"
        assert (
            _refused(lambda: vended.put_object(Bucket=BUCKET, Key=f"{a_loc}/data/x", Body=b"x"))
            == "AccessDenied"
        )
        assert (
            _refused(lambda: vended.put_object(Bucket=BUCKET, Key=f"{b_loc}/data/x", Body=b"x"))
            == "AccessDenied"
        )
        assert _refused(lambda: vended.delete_object(Bucket=BUCKET, Key=a_key)) == "AccessDenied"


# ----------------------- #


class TestCrossTenantByPath:
    """Battery 5 — tenant B's objects named by path from A's session, root key in ``AWS_*``."""

    @pytest.fixture(autouse=True)
    def _root_key_in_env(self, lake: LakeVendingStack, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", lake.root_access_key)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", lake.root_secret_key)
        monkeypatch.setenv("AWS_REGION", "local-01")

    @staticmethod
    def _b_paths(lake: LakeVendingStack, tenant_b: LakeTenant) -> tuple[str, str]:
        loaded = lake.load_table(tenant_b.principal, tenant_b.warehouse).json()
        return loaded["metadata-location"], f"{loaded['metadata']['location']}/data/*.parquet"

    def test_without_step_2_the_ambient_root_key_signs_and_b_is_read(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, tenant_b: LakeTenant
    ) -> None:
        """The leak §3.3 closes, shown first so the refusal below means something."""

        meta, _ = self._b_paths(lake, tenant_b)
        con = lake.session(tenant_a.principal, tenant_a.warehouse, global_s3_off=False)
        con.execute(f"SET s3_endpoint='{lake.s3_endpoint.split('://', 1)[1]}'")
        con.execute("SET s3_use_ssl=false")
        con.execute("SET s3_url_style='path'")
        con.execute("CALL enable_logging('HTTP')")

        assert con.execute(f"SELECT count(*) FROM iceberg_scan('{meta}')").fetchone() == (3,)
        # DuckDB 2's HTTP log no longer carries the SigV4 key id (D-060). The store refuses
        # unsigned reads (battery 1), so the read above was signed with the ambient key anyway.
        if not _ON_DUCKDB_2:
            assert lake.root_access_key in {r["key"] for r in _http_log(con)}

    def test_with_step_2_nothing_is_signed_with_the_root_key_and_b_is_refused(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, tenant_b: LakeTenant
    ) -> None:
        meta, glob = self._b_paths(lake, tenant_b)
        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)
        # An unmatched s3:// path goes to the default AWS endpoint, unsigned (EXECUTION-LOG D-027).
        # A local refusing proxy catches it, so the test does not leave the host.
        proxy, seen = _refusing_proxy()
        con.execute(f"SET http_proxy='127.0.0.1:{proxy}'")
        con.execute("CALL enable_logging('HTTP')")

        for sql in (
            f"SELECT * FROM iceberg_scan('{meta}')",
            f"SELECT * FROM read_parquet('{glob}')",
        ):
            with pytest.raises(duckdb.IOException):
                con.execute(sql).fetchall()

        assert lake.root_access_key not in {r["key"] for r in _http_log(con)}
        hosts = [_target_host(line) for line in seen]
        assert hosts and all(
            host == "s3.amazonaws.com" or host.endswith(".s3.amazonaws.com") for host in hosts
        )

    def test_the_p2_allowlist_refuses_inside_the_engine_before_any_request(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, tenant_b: LakeTenant
    ) -> None:
        meta, glob = self._b_paths(lake, tenant_b)
        con = lake.session(
            tenant_a.principal,
            tenant_a.warehouse,
            before_attach=_shipped_allowlist(lake, tenant_a),
        )
        con.execute("CALL enable_logging('HTTP')")

        for sql in (
            f"SELECT * FROM iceberg_scan('{meta}')",
            f"SELECT * FROM read_parquet('{glob}')",
        ):
            with pytest.raises(duckdb.PermissionException):
                con.execute(sql).fetchall()
        assert _http_log(con) == []  # logging starts after ATTACH: not even a catalog request


def _target_host(request_line: str) -> str:
    """The host a proxy request line names: ``CONNECT host:443 …`` or ``GET http://host/… …``."""

    target = request_line.split()[1]
    return urlsplit(target if "://" in target else f"//{target}").hostname or ""


def _refusing_proxy() -> tuple[int, list[str]]:
    """A one-line HTTP proxy that records each request line and answers 403."""

    seen: list[str] = []
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)

    def serve() -> None:
        while True:
            conn, _ = srv.accept()
            seen.append(conn.recv(65536).decode(errors="replace").split("\r\n", 1)[0])
            conn.sendall(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            conn.close()

    threading.Thread(target=serve, daemon=True).start()
    return srv.getsockname()[1], seen


# ----------------------- #


class TestRevocation:
    """Battery 11a — revoke A's grant mid-session; no restart, no eviction."""

    def test_the_next_catalog_statement_is_refused(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)

        lake.set_grant(tenant_a, tenant_a.principal, granted=False)
        try:
            with pytest.raises(duckdb.CatalogException, match='schema "gold" does not exist'):
                con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()
        finally:
            lake.set_grant(tenant_a, tenant_a.principal, granted=True)

    def test_a_by_path_read_inside_the_table_still_succeeds_on_the_residual_secret(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """EXECUTION-LOG D-025: on 1.5 vended secrets outlive their statement, so revocation is
        not total. On 2 the secret ends with its statement and the read has none (D-056)."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        # fetchall: the statement's transaction must commit for its vended secret to survive
        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchall() == [(3,)]
        path = _own_data_file(con)
        by_path = f"SELECT count(*) FROM read_parquet('{path}')"

        lake.set_grant(tenant_a, tenant_a.principal, granted=False)
        try:
            con.execute("CALL enable_logging('HTTP')")

            if _ON_DUCKDB_2:
                with pytest.raises(duckdb.HTTPException, match="No credentials are provided"):
                    con.execute(by_path).fetchall()
                return

            assert con.execute(by_path).fetchone() == (3,)
            assert any(r["status"] == "OK_200" and "/data/" in r["url"] for r in _http_log(con))
        finally:
            lake.set_grant(tenant_a, tenant_a.principal, granted=True)

    def test_a_re_grant_is_not_seen_by_the_session_that_saw_the_404(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """EXECUTION-LOG D-026: the attached catalog caches a missing schema with no further request."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        lake.set_grant(tenant_a, tenant_a.principal, granted=False)
        try:
            with pytest.raises(duckdb.CatalogException):
                con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()
        finally:
            lake.set_grant(tenant_a, tenant_a.principal, granted=True)
        con.execute("CALL enable_logging('HTTP')")

        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()
        assert _http_log(con) == []
        fresh = lake.session(tenant_a.principal, tenant_a.warehouse)
        assert fresh.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)

    def test_a_namespace_created_after_a_miss_stays_missing_but_a_new_table_appears(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """EXECUTION-LOG D-026, the other two cases: no revocation involved."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT * FROM lake.later_ns.t").fetchall()
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT * FROM lake.gold.later_table").fetchall()
        catalog = lake.admin_catalog(tenant_a)
        catalog.create_namespace_if_not_exists("later_ns")
        data = pa.table({"i": pa.array([1], pa.int64())})
        for ident in (("later_ns", "t"), ("gold", "later_table")):
            if not catalog.table_exists(ident):
                catalog.create_table(ident, schema=data.schema).append(data)

        with pytest.raises(duckdb.CatalogException, match='schema "later_ns" does not exist'):
            con.execute("SELECT * FROM lake.later_ns.t").fetchall()
        assert con.execute("SELECT * FROM lake.gold.later_table").fetchall() == [(1,)]
        fresh = lake.session(tenant_a.principal, tenant_a.warehouse)
        assert fresh.execute("SELECT * FROM lake.later_ns.t").fetchall() == [(1,)]

    def test_max_table_staleness_hides_a_revocation(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """Why §3.2 never sets it: the cached LoadTable outlives the grant."""

        con = lake.session(
            tenant_a.principal, tenant_a.warehouse, attach_options=", MAX_TABLE_STALENESS '1 hour'"
        )
        con.execute("CALL enable_logging('HTTP')")
        con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()
        lake.set_grant(tenant_a, tenant_a.principal, granted=False)
        try:
            assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)
        finally:
            lake.set_grant(tenant_a, tenant_a.principal, granted=True)
        assert sum("/tables/orders" in r["url"] for r in _http_log(con)) == 1


# ----------------------- #


class TestCredentialLifetime:
    """§3.2 — one LoadTable per autocommit statement; what happens to the secret it vends."""

    def test_every_statement_reloads_and_every_vended_secret_stays(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """On 2 none stays: the secret ends with its statement (D-056)."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        con.execute("CALL enable_logging('HTTP')")
        for _ in range(3):
            con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()

        assert sum("/tables/orders" in r["url"] for r in _http_log(con)) == 3
        assert con.execute(
            "SELECT count(*) FROM duckdb_secrets() WHERE type = 's3' AND name LIKE '__internal_ic_%'"
        ).fetchone() == ((0,) if _ON_DUCKDB_2 else (3,))

    def test_a_vended_secret_survives_only_a_committed_statement(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """On 1.5 the secret is a transactional catalog entry: commit keeps it, rollback
        discards it. On 2 neither keeps it (D-056)."""

        def secrets_after(run: Callable[[Any], object]) -> int:
            con = lake.session(tenant_a.principal, tenant_a.warehouse)
            run(con)
            return int(
                con.execute("SELECT count(*) FROM duckdb_secrets() WHERE type = 's3'").fetchone()[0]
            )

        def partial(con: Any) -> None:
            cur = con.cursor()
            cur.execute("SELECT * FROM lake.gold.orders").fetchmany(
                2
            )  # 3 rows: stream not exhausted
            cur.close()

        def exhausted(con: Any) -> None:
            cur = con.cursor()
            cur.execute("SELECT * FROM lake.gold.orders").fetchmany(4)  # the shell's cap+1 probe
            cur.close()

        assert secrets_after(exhausted) == (0 if _ON_DUCKDB_2 else 1)
        assert secrets_after(partial) == 0
        assert (
            secrets_after(
                lambda c: (
                    c.execute("BEGIN"),
                    c.execute("FROM lake.gold.orders").fetchall(),
                    c.execute("ROLLBACK"),
                )
            )
            == 0
        )

    def test_a_running_statements_secret_is_invisible_to_another_cursor(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """Uncommitted until the statement ends, so a drop elsewhere cannot take it (D-048).

        The version tripwire for the routed client's drop: an engine that lists a running
        statement's secret to other cursors fails here first."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        running = con.cursor()
        running.execute("SELECT * FROM lake.gold.orders").fetchmany(1)

        assert con.cursor().execute(_VENDED).fetchone() == (0,)
        running.fetchall()  # exhausted: the statement commits, and on 1.5 its secret with it
        assert con.cursor().execute(_VENDED).fetchone() == ((0,) if _ON_DUCKDB_2 else (1,))

    def test_a_scan_keeps_reading_while_another_statements_drop_runs(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """D-048 on a live scan: the drop takes the committed leftover, never the scan's secret."""

        schema = pa.schema([("id", pa.int64()), ("pad", pa.string())])
        table = lake.admin_catalog(tenant_a).create_table(("gold", "scan_d048"), schema=schema)
        for f in range(8):  # enough files that a paused stream still has requests to make
            ids = pa.array(range(f * 50_000, (f + 1) * 50_000), pa.int64())
            pad = pa.array([os.urandom(24).hex() for _ in range(50_000)], pa.string())
            table.append(pa.table({"id": ids, "pad": pad}, schema=schema))

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        con.execute("SET threads=1")
        con.execute("SET enable_external_file_cache=false")
        con.execute("CALL enable_logging('HTTP')")
        con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()  # 1.5: a leftover
        scan = con.cursor()
        scan.execute("SELECT id, pad FROM lake.gold.scan_d048")
        scan.fetchmany(1)
        requests_before = sum("/data/" in r["url"] for r in _http_log(con))

        client_module._drop_vended_secrets(con, threading.Lock())

        assert con.cursor().execute(_VENDED).fetchone() == (0,)  # the leftover went
        assert len(scan.fetchall()) == 8 * 50_000 - 1
        assert sum("/data/" in r["url"] for r in _http_log(con)) > requests_before
        # 1.5: the scan's own, now committed; 2: none (D-056)
        assert con.cursor().execute(_VENDED).fetchone() == ((0,) if _ON_DUCKDB_2 else (1,))

    def test_a_vended_secret_can_be_dropped_under_lock_configuration(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """On 2 there is nothing to drop: the routed client's drop finds no secret (D-056)."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        con.execute("SET lock_configuration=true")
        con.execute("SELECT count(*) FROM lake.gold.orders").fetchall()

        if _ON_DUCKDB_2:
            assert con.execute(_VENDED).fetchone() == (0,)
            client_module._drop_vended_secrets(con, threading.Lock())
            assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)
            return

        (name,) = con.execute(
            "SELECT name FROM duckdb_secrets() WHERE name LIKE '__internal_ic_%'"
        ).fetchone()

        con.execute(f'DROP TEMPORARY SECRET "{name}"')

        assert con.execute(
            "SELECT count(*) FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)


# ----------------------- #


class TestSessionHardening:
    """§3.4 and §9 q1 — which settings the catalog's own traffic survives."""

    def test_read_only_attach_is_honoured(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        con = lake.session(tenant_a.principal, tenant_a.warehouse, attach_options=", READ_ONLY")

        assert con.execute(
            "SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'"
        ).fetchone() == (True,)
        with pytest.raises(duckdb.InvalidInputException, match="read-only mode"):
            con.execute("INSERT INTO lake.gold.orders VALUES (99, 'x')")

    @pytest.mark.parametrize("where", ["before_attach", "after_attach"])
    def test_catalog_traffic_survives_disabled_filesystems(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, where: str
    ) -> None:
        lock = "SET disabled_filesystems='LocalFileSystem,HTTPFileSystem'"
        con = lake.session(
            tenant_a.principal,
            tenant_a.warehouse,
            before_attach=(lock,) if where == "before_attach" else (),
        )
        if where == "after_attach":
            con.execute(lock)
        con.execute("SET lock_configuration=true")

        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)
        assert con.cursor().execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)
        with pytest.raises(duckdb.PermissionException, match="HTTPFileSystem has been disabled"):
            con.execute("SELECT * FROM read_csv('http://127.0.0.1:9/x.csv')").fetchall()
        with pytest.raises(duckdb.PermissionException, match="LocalFileSystem has been disabled"):
            con.execute("SELECT * FROM read_text('/etc/hostname')").fetchall()

    def test_catalog_traffic_survives_the_p2_profile_set_before_attach(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        con = lake.session(
            tenant_a.principal,
            tenant_a.warehouse,
            before_attach=_shipped_allowlist(lake, tenant_a),
        )
        by_path = f"SELECT count(*) FROM read_parquet('{_own_data_file(con)}')"

        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchall() == [(3,)]
        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchall() == [(3,)]

        if _ON_DUCKDB_2:
            # No vended secret is left to match the path (D-056), so it heads for the public
            # AWS endpoint over HTTPS, and the allowlist holds no CA bundle (D-063).
            with pytest.raises(duckdb.PermissionException, match="CA certificate bundle"):
                con.execute(by_path).fetchall()
            return

        assert con.execute(by_path).fetchall() == [(3,)]

    def test_on_2_the_catalog_and_the_idp_must_be_on_the_allowlist(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """D-055: P1's prefix-only allowlist. DuckDB 2 checks the extension's own token request
        against it; 1.5 lets the catalog's traffic past the filesystem layer."""

        prefix_only = (
            f"SET allowed_directories=['s3://{BUCKET}/{tenant_a.key_prefix}/']",
            "SET enable_external_access=false",
        )

        if not _ON_DUCKDB_2:
            con = lake.session(tenant_a.principal, tenant_a.warehouse, before_attach=prefix_only)
            assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchall() == [(3,)]
            return

        with pytest.raises(duckdb.Error, match="Could not get token") as caught:
            lake.session(tenant_a.principal, tenant_a.warehouse, before_attach=prefix_only)
        assert "file system operations are disabled by configuration" in str(caught.value)

    def test_on_2_https_needs_the_ca_bundle_on_the_allowlist(
        self, tls_server: tuple[str, Path]
    ) -> None:
        """D-063: DuckDB 2 reads the CA bundle through the allowlist for every HTTPS request,
        and cannot with ``LocalFileSystem`` disabled. 1.5 reads it outside DuckDB's filesystem."""

        url, ca = tls_server
        origin = url.rsplit("/", 1)[0] + "/"

        def read(*, allow_ca: bool, disabled: str = "") -> object:
            con = duckdb.connect(":memory:", config={"allow_persistent_secrets": False})
            con.execute("LOAD httpfs")
            con.execute(f"SET GLOBAL ca_cert_file='{ca}'")
            con.execute(f"SET GLOBAL allowed_directories=['{origin}']")
            if allow_ca:
                con.execute(f"SET GLOBAL allowed_paths=['{ca}']")
            con.execute("SET GLOBAL enable_external_access=false")
            if disabled:
                con.execute(f"SET GLOBAL disabled_filesystems='{disabled}'")
            con.execute("SET GLOBAL lock_configuration=true")
            try:
                return con.execute(f"SELECT * FROM read_csv('{url}')").fetchall()
            except duckdb.PermissionException as error:
                return str(error)

        assert read(allow_ca=True) == [(1,)]  # the routed client's profile
        if not _ON_DUCKDB_2:
            assert read(allow_ca=False) == read(allow_ca=True, disabled="LocalFileSystem")
            assert read(allow_ca=False) == [(1,)]
            return

        assert "disabled by configuration" in str(read(allow_ca=False))
        assert "LocalFileSystem has been disabled" in str(
            read(allow_ca=True, disabled="LocalFileSystem")
        )

    def test_the_allowlist_resolves_no_dot_segments(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """D-073: a path matches by its spelling. ``catalog/../management`` passes as inside
        the catalog's tree and the request goes out; Lakekeeper refuses it unauthenticated."""

        con = lake.session(
            tenant_a.principal,
            tenant_a.warehouse,
            before_attach=_shipped_allowlist(lake, tenant_a),
        )
        origin = lake.catalog_uri.rsplit("/", 1)[0]

        for dots in ("..", "%2e%2e"):
            url = f"{lake.catalog_uri}/{dots}/management/v1/info"
            with pytest.raises(duckdb.HTTPException, match="401"):
                con.execute(f"SELECT * FROM read_text('{url}')").fetchall()

        with pytest.raises(duckdb.PermissionException, match="disabled by configuration"):
            con.execute(f"SELECT * FROM read_text('{origin}//catalog/v1/config')").fetchall()

    def test_a_redirect_is_followed_past_the_allowlist(self) -> None:
        """D-074: a 3xx from an allow-listed origin is followed without re-checking its target,
        and no setting turns redirects off. Pinned so a bump that re-checks them shows."""

        hits: list[str] = []

        class _Stub(http.server.BaseHTTPRequestHandler):
            def _answer(self, body: bool) -> None:  # noqa: FBT001
                hits.append(f"{self.server.server_address[1]}{self.path}")  # type: ignore[index]
                if self.path.startswith("/allowed/"):
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{off_list}/elsewhere.csv")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", "4")
                self.end_headers()
                if body:
                    self.wfile.write(b"a\n1\n")

            def do_GET(self) -> None:  # noqa: N802
                self._answer(True)

            def do_HEAD(self) -> None:  # noqa: N802
                self._answer(False)

            def log_message(self, *args: Any) -> None:
                pass

        servers = [http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Stub) for _ in range(2)]
        for server in servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        allowed, off_list = (server.server_address[1] for server in servers)

        try:
            con = duckdb.connect(":memory:", config={"allow_persistent_secrets": False})
            con.execute("LOAD httpfs")
            con.execute(f"SET GLOBAL allowed_directories=['http://127.0.0.1:{allowed}/allowed/']")
            con.execute("SET GLOBAL enable_external_access=false")
            con.execute("SET GLOBAL lock_configuration=true")

            rows = con.execute(
                f"SELECT * FROM read_csv('http://127.0.0.1:{allowed}/allowed/x.csv')"
            ).fetchall()

            assert rows == [(1,)]
            assert f"{off_list}/elsewhere.csv" in hits
            assert con.execute(
                "SELECT count(*) FROM duckdb_settings() "
                "WHERE name ILIKE '%redirect%' OR name ILIKE '%follow%'"
            ).fetchone() == (0,)

        finally:
            for server in servers:
                server.shutdown()

    def test_the_p2_allowlist_must_be_set_before_external_access_goes_off(self) -> None:
        con = duckdb.connect(":memory:")
        con.execute("SET enable_external_access=false")

        with pytest.raises(duckdb.InvalidInputException, match="Cannot change allowed_directories"):
            con.execute("SET allowed_directories=['s3://lake/x/']")

    def test_the_external_file_cache_is_per_instance(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        warm = lake.session(tenant_a.principal, tenant_a.warehouse)
        warm.execute("SELECT count(*) FROM lake.gold.orders").fetchall()
        cold = duckdb.connect(":memory:")

        assert warm.execute("SELECT count(*) FROM duckdb_external_file_cache()").fetchone()[0] > 0
        assert cold.execute("SELECT count(*) FROM duckdb_external_file_cache()").fetchone() == (0,)


# ----------------------- #


class TestRefusalTaxonomy:
    """Battery 16 / §4.5 — the exception class each catalog or store refusal arrives as."""

    def test_attach_refusals_are_the_bare_base_class(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, tenant_b: LakeTenant
    ) -> None:
        cases = [
            (lake.ungranted, tenant_a.warehouse, "NotFound_404"),
            (tenant_a.principal, tenant_b.warehouse, "NotFound_404"),
            (
                LakePrincipal(tenant_a.principal.client_id, "wrong", tenant_a.principal.user_id),
                tenant_a.warehouse,
                "Unauthorized_401",
            ),
        ]
        for principal, warehouse, status in cases:
            with pytest.raises(duckdb.Error, match=status) as caught:
                lake.session(principal, warehouse)

            assert type(caught.value) is duckdb.Error

    def test_a_describe_only_principal_gets_metadata_and_no_credentials(
        self, lake: LakeVendingStack, tenant_a: LakeTenant, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # With a region in the environment the scan would carry on, unsigned, to the default AWS
        # endpoint (D-027); without one it stops in the engine. Either way: no credentials, no rows.
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        lake.set_grant(tenant_a, lake.ungranted, granted=True, privilege="describe")
        try:
            loaded = lake.load_table(lake.ungranted, tenant_a.warehouse)
            con = lake.session(lake.ungranted, tenant_a.warehouse)

            assert loaded.status_code == 200
            assert "storage-credentials" not in loaded.json()
            with pytest.raises(duckdb.Error) as caught:
                con.execute("SELECT * FROM lake.gold.orders").fetchall()
            assert type(caught.value) is duckdb.Error
        finally:
            lake.set_grant(tenant_a, lake.ungranted, granted=False, privilege="describe")

    def test_a_select_only_write_is_refused_by_the_store_and_the_catalog(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        con = lake.session(tenant_a.principal, tenant_a.warehouse)

        with pytest.raises(duckdb.HTTPException, match="403"):
            con.execute("INSERT INTO lake.gold.orders VALUES (99, 'x')")
        with pytest.raises(duckdb.Error, match="Forbidden_403") as caught:
            con.execute("CREATE TABLE lake.gold.t_new (i INT)")
        # On 2 the catalog's refusal is an HTTPException (D-060).
        assert type(caught.value) is (duckdb.HTTPException if _ON_DUCKDB_2 else duckdb.Error)
        assert con.execute("SELECT count(*) FROM lake.gold.orders").fetchone() == (3,)

    def test_an_unknown_first_part_is_a_nested_namespace_on_2(
        self, lake: LakeVendingStack, tenant_a: LakeTenant
    ) -> None:
        """D-058, the engine's half: 1.5 refuses an unknown catalog at bind; 2 reads it as a
        nested namespace and resolves only its last level, in the tenant's own warehouse. A table
        function taking the name or SQL text does the same (D-072). The dynamic-read
        classification refuses all of these on both (D-066, D-072)."""

        con = lake.session(tenant_a.principal, tenant_a.warehouse)
        con.execute("USE lake.gold")

        for sql in (
            "SELECT DISTINCT tenant FROM zzz.gold.orders",
            "SELECT DISTINCT tenant FROM wh_tenant_b.gold.orders",
            "SELECT DISTINCT tenant FROM query('FROM zzz.gold.orders')",
            "SELECT DISTINCT tenant FROM query_table('wh_tenant_b.gold.orders')",
        ):
            if _ON_DUCKDB_2:
                assert con.execute(sql).fetchall() == [("tenant-a",)]
            else:
                with pytest.raises(duckdb.BinderException, match="does not exist"):
                    con.execute(sql).fetchall()
