"""RoutedDuckDbClient: construction refusals and the per-tenant pool, over a fake engine.

The hardening sequence and its attestation need a real catalog, so they run against the
Lakekeeper stack in ``tests/integration/test_forze_duckdb/test_lake_routed_reads.py``. Here the
inner client is a stand-in, which is what lets the pool's own behaviour — eviction under a
running statement, the guarded drain, the process-wide admission semaphore — be pinned
exactly, including under perturbed schedules on the DST loop (RFC 0078 §4.6).
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import random
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from secrets import token_hex
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import duckdb
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from pydantic import SecretStr

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_dst.runtime import run_simulation
from forze_dst.scheduler import PCTReorderer
from forze_duckdb import (
    DuckDbConfig,
    DuckLakeCatalogCredentials,
    DuckLakeTenantCatalog,
    LakeCatalogCredentials,
    LakeTenantCatalog,
    RoutedDuckDbClient,
)
from forze_duckdb.adapters import dynamic_read_error
from forze_duckdb.kernel.client import routed_client

# ----------------------- #

_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_C = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
_TIMEOUT = timedelta(seconds=5)


def _self_signed_pem() -> str:
    signer = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ca.test")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(signer.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(signer, hashes.SHA256())
    )

    return cert.public_bytes(serialization.Encoding.PEM).decode()


_PEM = _self_signed_pem()
"""A CA certificate OpenSSL loads, as a ``ca_cert_file`` must hold."""

_tenant: contextvars.ContextVar[UUID | None] = contextvars.ContextVar("tenant", default=None)


class _MemSecrets:
    """Client-credential payloads by tenant; ``rotate`` swaps one tenant's secret.

    Every secret is generated, so ``first`` records the one each tenant started with.
    """

    def __init__(self, *tenants: UUID) -> None:
        self.payloads = {t: {"client_id": f"id-{t.hex[:4]}"} for t in tenants}

        for tenant in tenants:
            self.rotate(tenant, uuid4().hex)

        self.first = {t: payload["client_secret"] for t, payload in self.payloads.items()}

    def rotate(self, tenant: UUID, secret: str) -> None:
        self.payloads[tenant] = {**self.payloads[tenant], "client_secret": secret}

    async def resolve_str(self, ref: SecretRef) -> str:
        return json.dumps(self.payloads[UUID(ref.path)])

    async def exists(self, ref: SecretRef) -> bool:
        return UUID(ref.path) in self.payloads


class _FakeEngine:
    """Stands in for one tenant's DuckDbClient and records what happened to it."""

    built: ClassVar[list[_FakeEngine]] = []

    def __init__(self) -> None:
        self.closed = False
        self.retired = False
        self.statements: list[str] = []
        self.admission: asyncio.Semaphore | None = None
        self.secret: str | None = None
        _FakeEngine.built.append(self)

    async def initialize(self, database: str, **kwargs: Any) -> None:
        self.database = database
        self.admission = kwargs["admission"]
        self.install_extensions = kwargs["install_extensions"]
        self.drop_vended_secrets = kwargs["drop_vended_secrets"]
        self.extensions = tuple(kwargs["extensions"])
        self.connect_config = kwargs["connect_config"]

    async def run_command(self, command: str, *args: Any, **kwargs: Any) -> None:
        for key in ("CLIENT_SECRET '", "PASSWORD '"):
            if command.startswith("CREATE SECRET") and key in command:
                self.secret = command.split(key, 1)[1].split("'", 1)[0]
        self.statements.append(command)

    async def run_dynamic(self, statement: str, *args: Any, **kwargs: Any) -> list[Any]:
        assert not self.closed, "a statement ran on a closed instance"
        assert self.admission is not None

        if statement == "retire":  # stands in for a read whose vended-secret drop failed
            self.retired = True
            return []

        async with self.admission:
            await asyncio.sleep(float(statement))

        assert not self.closed, "the instance closed under a running statement"
        return [{"secret": self.secret}]

    async def run_query(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self.statements.append(sql)
        return [{"sql": sql}]

    async def run_query_all_pages(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self.statements.append(sql)
        return [{"sql": sql}]

    async def run_query_streamed(
        self, sql: str, *args: Any, **kwargs: Any
    ) -> AsyncIterator[list[Any]]:
        self.statements.append(sql)
        yield [{"sql": sql}]

    async def health(self) -> tuple[str, bool]:
        return "ok", True

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[_FakeEngine]]:
    _FakeEngine.built = []
    monkeypatch.setattr(routed_client, "DuckDbClient", _FakeEngine)
    monkeypatch.setattr(routed_client, "_attest", _attested)
    monkeypatch.setattr(routed_client, "_ducklake_problems", _attested)
    yield _FakeEngine


async def _attested(client: Any, *args: Any) -> list[str]:
    return []


def _catalog(tenant: UUID) -> LakeTenantCatalog:
    return LakeTenantCatalog(
        uri="http://catalog.test/catalog",
        warehouse=f"wh-{tenant.hex[:4]}",
        oauth2_server_uri="http://idp.test/token",
        oauth2_scope="profile",
        storage_prefix=f"s3://lake/{tenant.hex[:4]}/",
    )


def _routed(secrets: _MemSecrets, **kwargs: Any) -> RoutedDuckDbClient:
    return RoutedDuckDbClient(
        secrets=secrets,
        secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
        tenant_provider=_tenant.get,
        catalog_for_tenant=_catalog,
        **kwargs,
    )


async def _read(routed: RoutedDuckDbClient, tenant: UUID, seconds: float = 0.0) -> Any:
    _tenant.set(tenant)
    return await routed.run_dynamic(str(seconds), row_probe=2, timeout=_TIMEOUT)


# ....................... #


class TestConstruction:
    def test_unguarded_eviction_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _routed(_MemSecrets(_A), guarded=False)

        assert ei.value.code == "duckdb_routed_client_unguarded"
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize("field", ["object_stores", "secrets_sql", "sources", "bootstrap_sql"])
    def test_there_is_no_field_for_static_credentials_or_raw_statements(self, field: str) -> None:
        with pytest.raises(TypeError, match=field):
            _routed(_MemSecrets(_A), **{field: ()})

    @pytest.mark.parametrize(
        "http_timeout", [timedelta(0), timedelta(milliseconds=500), timedelta(seconds=1.5)]
    )
    def test_http_timeout_must_be_whole_seconds(self, http_timeout: timedelta) -> None:
        with pytest.raises(CoreException) as ei:
            _routed(_MemSecrets(_A), http_timeout=http_timeout)

        assert ei.value.code == "duckdb_routed_client_invalid"

    def test_a_process_wide_limit_below_one_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _routed(_MemSecrets(_A), max_concurrent_queries_total=0)

        assert ei.value.code == "duckdb_routed_client_invalid"

    @pytest.mark.parametrize("prefix", ["s3://lake/tenant-a", "/data/tenant-a/", "gs://lake/a/"])
    def test_a_storage_prefix_must_be_an_s3_directory(self, prefix: str) -> None:
        with pytest.raises(CoreException) as ei:
            LakeTenantCatalog(
                uri="u", warehouse="w", oauth2_server_uri="o", oauth2_scope="s",
                storage_prefix=prefix,
            )

        assert ei.value.code == "duckdb_lake_catalog_invalid"

    @pytest.mark.parametrize("field", ["uri", "oauth2_server_uri"])
    @pytest.mark.parametrize("value", ["s3://lake/", "/catalog", "catalog.test/catalog", "http://"])
    def test_the_catalog_and_the_idp_must_be_http_urls(self, field: str, value: str) -> None:
        """Both go into the session's allowlist, where anything else would widen it."""

        urls = {"uri": "https://catalog.test/catalog", "oauth2_server_uri": "https://idp.test/t"}

        with pytest.raises(CoreException) as ei:
            LakeTenantCatalog(
                **{**urls, field: value}, warehouse="w", oauth2_scope="s",
                storage_prefix="s3://lake/a/",
            )

        assert ei.value.code == "duckdb_lake_catalog_invalid"
        assert ei.value.details == {field: value}

    @pytest.mark.parametrize(
        "content",
        [
            "",
            "not a certificate\n",
            "-----BEGIN PRIVATE KEY-----\n",
            "-----BEGIN CERTIFICATE-----\n",
            _PEM[: len(_PEM) // 2],
        ],
        ids=["empty", "text", "key", "marker", "truncated"],
    )
    def test_a_ca_bundle_without_a_pem_certificate_is_refused(
        self, tmp_path: Any, content: str
    ) -> None:
        """D-075: DuckDB 2 hands the file to curl for every request; a wrong one fails each."""

        bundle = tmp_path / "ca.pem"
        bundle.write_text(content)

        with pytest.raises(CoreException) as ei:
            _routed(_MemSecrets(_A), ca_cert_file=str(bundle))

        assert ei.value.code == "duckdb_routed_client_invalid"
        assert ei.value.details == {"ca_cert_file": str(bundle.resolve())}

    def test_a_missing_ca_bundle_is_refused(self, tmp_path: Any) -> None:
        with pytest.raises(CoreException) as ei:
            _routed(_MemSecrets(_A), ca_cert_file=str(tmp_path / "absent.pem"))

        assert ei.value.code == "duckdb_routed_client_invalid"


# ....................... #


class TestInstances:
    async def test_the_session_allow_lists_the_prefix_the_catalog_the_idp_and_the_ca_bundle(
        self, fake_engine: type[_FakeEngine], tmp_path: Any
    ) -> None:
        """D-062, D-063: one allowlist bounds every path, and no filesystem is disabled."""

        bundle = tmp_path / "ca.pem"
        bundle.write_text(_PEM)
        link = tmp_path / "ca-link.pem"
        link.symlink_to(bundle)
        routed = _routed(_MemSecrets(_A), ca_cert_file=str(link))
        await routed.startup()

        await _read(routed, _A)

        (engine,) = fake_engine.built
        real = str(bundle.resolve())
        assert f"SET GLOBAL ca_cert_file='{real}'" in engine.statements
        assert (
            "SET GLOBAL allowed_directories=['http://catalog.test/catalog/', "
            f"'s3://lake/{_A.hex[:4]}/']"
        ) in engine.statements
        assert f"SET GLOBAL allowed_paths=['{real}', 'http://idp.test/token']" in engine.statements
        assert engine.statements.index("SET GLOBAL enable_external_access=false") < next(
            i for i, s in enumerate(engine.statements) if s.startswith("CREATE SECRET")
        )
        assert not any("disabled_filesystems" in s for s in engine.statements)
        await routed.close()

    async def test_an_instance_is_built_load_only_with_the_shared_admission(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        routed = _routed(_MemSecrets(_A, _B))
        await routed.startup()

        await _read(routed, _A)
        await _read(routed, _B)

        first, second = fake_engine.built
        assert first.database == second.database == ":memory:"
        assert first.install_extensions is False
        assert first.drop_vended_secrets is True
        assert first.admission is second.admission is not None
        assert first.statements[-1] == "SET GLOBAL lock_configuration=true"
        await routed.close()

    async def test_an_empty_client_id_is_refused_before_any_instance(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        secrets = _MemSecrets(_A)
        secrets.payloads[_A]["client_id"] = ""
        routed = _routed(secrets)
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value.code == "duckdb_lake_client_id_empty"
        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert fake_engine.built == []
        await routed.close()

    async def test_a_failed_attestation_closes_the_instance_and_serves_nothing(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def unlocked(client: Any, *args: Any) -> list[str]:
            return ["setting lock_configuration"]

        monkeypatch.setattr(routed_client, "_attest", unlocked)
        routed = _routed(_MemSecrets(_A))
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value.code == "duckdb_session_attestation_failed"
        assert ei.value.details == {
            "tenant_id": str(_A),
            "problems": ["setting lock_configuration"],
        }
        (instance,) = fake_engine.built
        assert instance.closed
        await routed.close()

    async def test_a_failed_build_step_is_a_configuration_error_not_the_statements(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A missing extension reaches the caller as the deployment's fault, never as a
        statement taxonomy code."""

        async def no_iceberg(engine: _FakeEngine, command: str, *args: Any, **kwargs: Any) -> None:
            if command.startswith("CREATE SECRET"):
                raise duckdb.InvalidInputException("Secret type 'iceberg' does not exist")

        monkeypatch.setattr(_FakeEngine, "run_command", no_iceberg)
        routed = _routed(_MemSecrets(_A))
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value.code == "duckdb_session_setup_failed"
        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert ei.value.details == {
            "tenant_id": str(_A),
            "error": "Secret type 'iceberg' does not exist",
        }
        assert dynamic_read_error(ei.value, route="r") is ei.value  # no statement code
        (instance,) = fake_engine.built
        assert instance.closed
        await routed.close()

    @pytest.mark.parametrize("spelling", ["raw", "sql_escaped", "statement_echo"])
    async def test_a_failed_build_step_never_reports_the_client_secret(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch, spelling: str
    ) -> None:
        """The error's text is redacted, and the engine's error is not chained, so neither the
        details nor a log of the cause carry the principal's secret, even in part."""

        value = token_hex(16)
        leak = value[:16] + "'" + value[16:]  # an apostrophe, so the escaped spelling differs
        escaped = leak.replace("'", "''")
        engine_errors: list[str] = []

        async def failing(engine: _FakeEngine, command: str, *args: Any, **kwargs: Any) -> None:
            if not command.startswith("CREATE SECRET"):
                return

            if spelling == "statement_echo":
                # DuckDB's own parser error, which quotes a window of the statement around the
                # error: here it cuts the secret short, so an exact match would miss it.
                broken = command.replace(", OAUTH2_SCOPE", " OAUTH2_SCOPE", 1)

                try:
                    duckdb.connect(":memory:").execute(broken)

                except duckdb.ParserException as error:
                    engine_errors.append(str(error))
                    raise

            engine_errors.append(f"Invalid secret {leak if spelling == 'raw' else escaped}")
            raise duckdb.InvalidInputException(engine_errors[-1])

        monkeypatch.setattr(_FakeEngine, "run_command", failing)
        principals = _MemSecrets(_A)
        principals.rotate(_A, leak)
        routed = _routed(principals)
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        windows = {text[i : i + 6] for text in (leak, escaped) for i in range(len(text) - 5)}
        (engine_error,) = engine_errors
        assert any(w in engine_error for w in windows)  # the engine's text did carry it
        reported = f"{ei.value} {ei.value.details}"
        assert not any(w in reported for w in windows)
        assert ei.value.code == "duckdb_session_setup_failed"
        assert ei.value.__cause__ is None and ei.value.__suppress_context__
        await routed.close()

    def test_an_empty_secret_still_loses_the_statement_quote(self) -> None:
        text = "Parser Error: syntax error at or near \",\"\n\nLINE 1: ...SECRET '' , ,\n   ^"

        assert routed_client._redacted(text, "") == 'Parser Error: syntax error at or near ","'

    async def test_a_retired_instance_is_evicted_and_its_successor_spared(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        routed = _routed(_MemSecrets(_A))
        await routed.startup()
        holding = asyncio.create_task(_read(routed, _A, 0.2))  # keeps the first instance busy
        await asyncio.sleep(0.05)

        _tenant.set(_A)
        await routed.run_dynamic("retire", row_probe=2, timeout=_TIMEOUT)
        await _read(routed, _A)  # served by a rebuilt instance
        await holding  # leaves the retired instance last, after its successor exists

        first, second = fake_engine.built
        assert first.retired and not second.retired
        assert routed._peek_client(_A) is second
        await routed.close()
        assert first.closed

    async def test_evict_tenant_rebuilds_with_rotated_credentials(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        secrets = _MemSecrets(_A)
        routed = _routed(secrets)
        await routed.startup()
        assert await _read(routed, _A) == [{"secret": secrets.first[_A]}]

        rotated = uuid4().hex
        secrets.rotate(_A, rotated)
        await routed.evict_tenant(_A)

        assert await _read(routed, _A) == [{"secret": rotated}]
        assert [engine.closed for engine in fake_engine.built] == [True, False]
        await routed.close()

    async def test_lru_eviction_lets_a_running_statement_finish(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        secrets = _MemSecrets(_A, _B)
        routed = _routed(secrets, max_cached_tenants=1)
        await routed.startup()

        running = asyncio.create_task(_read(routed, _A, 0.05))
        await asyncio.sleep(0.01)
        await _read(routed, _B)  # evicts A while A's statement runs

        a_engine = fake_engine.built[0]
        assert not a_engine.closed  # draining, not closed
        assert await running == [{"secret": secrets.first[_A]}]
        assert a_engine.closed  # closed once its last statement returned
        await routed.close()

    async def test_the_admission_semaphore_bounds_statements_across_tenants(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        routed = _routed(_MemSecrets(_A, _B, _C), max_concurrent_queries_total=2)
        await routed.startup()
        loop = asyncio.get_running_loop()
        started = loop.time()

        await asyncio.gather(*(_read(routed, t, 0.05) for t in (_A, _B, _C)))

        assert loop.time() - started >= 0.1  # the third statement waited for a slot
        await routed.close()

    async def test_a_classified_error_from_a_build_step_passes_unchanged(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        refused = exc.infrastructure("no slot in time", code="duckdb_query_timeout")

        async def timed_out(engine: _FakeEngine, command: str, *args: Any, **kwargs: Any) -> None:
            raise refused

        monkeypatch.setattr(_FakeEngine, "run_command", timed_out)
        routed = _routed(_MemSecrets(_A))
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value is refused
        (instance,) = fake_engine.built
        assert instance.closed
        await routed.close()

    async def test_every_call_runs_on_the_bound_tenants_instance(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        routed = _routed(_MemSecrets(_A))
        await routed.startup()
        _tenant.set(_A)

        assert await routed.health() == ("ok", True)
        assert await routed.run_query("q1") == [{"sql": "q1"}]
        assert await routed.run_query_all_pages("q2") == [{"sql": "q2"}]
        assert [batch async for batch in routed.run_query_streamed("q3")] == [[{"sql": "q3"}]]
        await routed.run_command("q4")

        (instance,) = fake_engine.built
        assert instance.statements[-4:] == ["q1", "q2", "q3", "q4"]
        await routed.close()


class TestTeardownAndRemapping:
    """A stuck close never holds a caller; a remapped catalog rebuilds the instance."""

    @pytest.fixture
    def stuck_close(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch
    ) -> asyncio.Event:
        """Every instance's close waits for the gate, as one with a hung worker does."""

        gate = asyncio.Event()

        async def close(engine: _FakeEngine) -> None:
            await gate.wait()
            engine.closed = True

        monkeypatch.setattr(_FakeEngine, "close", close)
        return gate

    async def test_the_last_caller_of_a_drained_instance_is_not_held_by_its_close(
        self, fake_engine: type[_FakeEngine], stuck_close: asyncio.Event
    ) -> None:
        routed = _routed(_MemSecrets(_A, _B), max_cached_tenants=1)
        await routed.startup()
        running = asyncio.create_task(_read(routed, _A, 0.05))
        await asyncio.sleep(0.01)
        await asyncio.wait_for(_read(routed, _B), 1)  # A drains while its statement runs

        assert await asyncio.wait_for(running, 1)  # the scope exit does not await the close
        assert not fake_engine.built[0].closed

        stuck_close.set()
        await routed.close()
        assert all(engine.closed for engine in fake_engine.built)

    async def test_an_overflow_eviction_does_not_stall_another_tenants_first_call(
        self, fake_engine: type[_FakeEngine], stuck_close: asyncio.Event
    ) -> None:
        routed = _routed(_MemSecrets(_A, _B), max_cached_tenants=1)
        await routed.startup()
        await _read(routed, _A)

        assert await asyncio.wait_for(_read(routed, _B), 1)  # evicts the idle A

        close = asyncio.create_task(routed.close())
        await asyncio.sleep(0.01)
        assert not close.done()  # close waits for the outstanding teardown

        stuck_close.set()
        await close
        assert all(engine.closed for engine in fake_engine.built)

    async def test_a_failed_background_close_is_logged(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def broken(engine: _FakeEngine) -> None:
            raise RuntimeError("close failed")

        monkeypatch.setattr(_FakeEngine, "close", broken)
        log = MagicMock()
        monkeypatch.setattr(routed_client, "logger", log)
        routed = _routed(_MemSecrets(_A))
        await routed.startup()
        await _read(routed, _A)

        await routed.evict_tenant(_A)
        await routed.close()  # waits for the teardown, and does not raise its error

        log.warning.assert_called_once()

    async def test_a_remapped_catalog_rebuilds_the_instance(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        warehouses = {_A: "wh-old"}

        def catalog(tenant: UUID) -> LakeTenantCatalog:
            return LakeTenantCatalog(
                uri="http://catalog.test/catalog",
                warehouse=warehouses[tenant],
                oauth2_server_uri="http://idp.test/token",
                oauth2_scope="profile",
                storage_prefix="s3://lake/a/",
            )

        routed = RoutedDuckDbClient(
            secrets=_MemSecrets(_A),
            secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
            tenant_provider=_tenant.get,
            catalog_for_tenant=catalog,
            fingerprint_ttl=timedelta(milliseconds=1),
        )
        await routed.startup()
        await _read(routed, _A)
        warehouses[_A] = "wh-new"
        await asyncio.sleep(0.01)

        await _read(routed, _A)

        old, new = fake_engine.built
        assert old.closed and not new.closed
        assert any("ATTACH 'wh-new'" in statement for statement in new.statements)
        await routed.close()


# ....................... #


class _DuckLakeSecrets:
    """Metadata-role payloads by tenant, with a store key for each tenant in *store*."""

    def __init__(self, *tenants: UUID, store: tuple[UUID, ...] = ()) -> None:
        self.payloads: dict[UUID, dict[str, Any]] = {
            t: {"user": f"r_{t.hex[:4]}", "password": token_hex(16)} for t in tenants
        }

        for tenant in store:
            self.payloads[tenant]["store"] = {
                "access_key_id": f"k-{tenant.hex[:4]}",
                "secret_access_key": token_hex(16),
                "endpoint": "store.test:9000",
                "url_style": "path",
                "use_ssl": False,
            }

    async def resolve_str(self, ref: SecretRef) -> str:
        return json.dumps(self.payloads[UUID(ref.path)])

    async def exists(self, ref: SecretRef) -> bool:
        return UUID(ref.path) in self.payloads


def _ducklake(tenant: UUID, *, s3: bool = False) -> DuckLakeTenantCatalog:
    key = tenant.hex[:4]

    return DuckLakeTenantCatalog(
        host="meta.test",
        database="lakemeta",
        metadata_schema=f"t_{key}",
        data_path=f"s3://lake/{key}/" if s3 else f"/data/{key}/",
    )


def _routed_ducklake(secrets: Any, catalog: Any = _ducklake, **kwargs: Any) -> RoutedDuckDbClient:
    return RoutedDuckDbClient(
        secrets=secrets,
        secret_ref_for_tenant=lambda tenant: SecretRef(path=str(tenant)),
        tenant_provider=_tenant.get,
        catalog_for_tenant=catalog,
        **kwargs,
    )


def _catalog_with(data_path: str = "/data/a/", **fields: Any) -> DuckLakeTenantCatalog:
    return DuckLakeTenantCatalog(
        **{"host": "h", "database": "d", "metadata_schema": "m", "data_path": data_path, **fields}
    )


class TestDuckLakeCatalog:
    @pytest.mark.parametrize("path", ["/data/a/", "/data/", "s3://lake/a/", "s3://lake/"])
    def test_a_data_path_is_an_absolute_directory_or_an_s3_prefix(self, path: str) -> None:
        assert _catalog_with(path).data_path == path

    @pytest.mark.parametrize(
        "path",
        [
            "/data/a",
            "data/a/",
            "/data/../a/",
            "/data/./a/",
            "/data//a/",
            "/",
            "s3://lake",
            "s3:///a/",
            "http://store.test/a/",
            "",
        ],
    )
    def test_a_data_path_the_allowlist_would_misread_is_refused(self, path: str) -> None:
        with pytest.raises(CoreException) as ei:
            _catalog_with(path)

        assert ei.value.code == "duckdb_lake_catalog_invalid"

    @pytest.mark.parametrize("field", ["host", "database", "metadata_schema"])
    def test_the_metadata_database_must_be_named(self, field: str) -> None:
        with pytest.raises(CoreException) as ei:
            _catalog_with(**{field: ""})

        assert ei.value.code == "duckdb_lake_catalog_invalid"

    @pytest.mark.parametrize("port", [0, 65536])
    def test_the_port_must_be_a_tcp_port(self, port: int) -> None:
        with pytest.raises(CoreException) as ei:
            _catalog_with(port=port)

        assert ei.value.code == "duckdb_lake_catalog_invalid"


class TestDuckLakeInstances:
    async def test_the_password_goes_in_the_unnamed_secret_never_the_attach_string(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        """D-099: ``duckdb_databases()`` hands the attach string to any statement."""

        secrets = _DuckLakeSecrets(_A)
        routed = _routed_ducklake(secrets)
        await routed.startup()

        await _read(routed, _A)

        (engine,) = fake_engine.built
        password = secrets.payloads[_A]["password"]
        (holding,) = [s for s in engine.statements if password in s]
        assert holding.startswith("CREATE SECRET (TYPE postgres, HOST 'meta.test', PORT 5432, ")
        attach = next(s for s in engine.statements if s.startswith("ATTACH"))
        assert attach == (
            "ATTACH 'ducklake:postgres:' AS lake "
            "(DATA_PATH '/data/aaaa/', METADATA_SCHEMA 't_aaaa', READ_ONLY)"
        )
        steps = (
            "SET GLOBAL allowed_directories=['/data/aaaa/']",
            "SET GLOBAL allowed_paths=[]",
            holding,
            attach,
            "SET GLOBAL enable_external_access=false",
        )
        order = [engine.statements.index(step) for step in steps]
        assert order == sorted(order)
        assert engine.statements[-1] == "SET GLOBAL lock_configuration=true"
        await routed.close()

    async def test_a_local_lake_loads_no_httpfs_and_keeps_persistent_secrets_off(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        routed = _routed_ducklake(_DuckLakeSecrets(_A))
        await routed.startup()

        await _read(routed, _A)

        (engine,) = fake_engine.built
        assert engine.extensions == ("ducklake", "postgres")
        assert engine.install_extensions is False
        assert engine.drop_vended_secrets is False
        assert engine.connect_config["allow_persistent_secrets"] is False
        assert not any("s3_" in s or "http_timeout" in s for s in engine.statements)
        await routed.close()

    async def test_a_lake_on_s3_scopes_the_store_key_to_its_data_path(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        secrets = _DuckLakeSecrets(_A, store=(_A,))
        routed = _routed_ducklake(secrets, catalog=lambda t: _ducklake(t, s3=True))
        await routed.startup()

        await _read(routed, _A)

        (engine,) = fake_engine.built
        assert engine.extensions == ("ducklake", "postgres", "httpfs")
        (store,) = [s for s in engine.statements if "SECRET lake_store" in s]
        assert "TYPE S3" in store and "SCOPE 's3://lake/aaaa/'" in store
        assert engine.statements.index("SET GLOBAL enable_global_s3_configuration=false") < (
            engine.statements.index(store)
        )
        assert "SET GLOBAL allowed_directories=['s3://lake/aaaa/']" in engine.statements
        assert "SET GLOBAL http_timeout=5" in engine.statements
        await routed.close()

    @pytest.mark.parametrize(("s3", "store"), [(False, True), (True, False)])
    async def test_a_store_key_that_does_not_fit_the_data_path_is_refused(
        self, fake_engine: type[_FakeEngine], s3: bool, store: bool
    ) -> None:
        secrets = _DuckLakeSecrets(_A, store=(_A,) if store else ())
        routed = _routed_ducklake(secrets, catalog=lambda t: _ducklake(t, s3=s3))
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value.code == "duckdb_lake_store_credentials_mismatch"
        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert fake_engine.built == []
        await routed.close()

    async def test_each_tenant_resolves_the_secret_its_catalog_kind_needs(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        secrets = _MemSecrets(_A)
        secrets.payloads.update(_DuckLakeSecrets(_B).payloads)
        routed = _routed_ducklake(
            secrets, catalog=lambda t: _catalog(t) if t == _A else _ducklake(t)
        )
        await routed.startup()

        await _read(routed, _A)
        await _read(routed, _B)

        iceberg, ducklake = fake_engine.built
        assert any("TYPE iceberg" in s for s in iceberg.statements)
        assert any("TYPE postgres" in s for s in ducklake.statements)
        await routed.close()

    async def test_a_catalog_that_changed_kind_since_its_secret_builds_nothing(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        routed = _routed_ducklake(_DuckLakeSecrets(_A))
        creds = LakeCatalogCredentials(client_id="id", client_secret=SecretStr(token_hex(16)))

        with pytest.raises(CoreException) as ei:
            await routed.initialize_client(_A, creds)

        assert ei.value.code == "duckdb_lake_catalog_kind_changed"
        assert fake_engine.built == []

    def test_rotating_the_password_or_the_store_key_changes_the_fingerprint(self) -> None:
        routed = _routed_ducklake(_DuckLakeSecrets(_A))
        base = DuckLakeCatalogCredentials.model_validate(
            _DuckLakeSecrets(_A, store=(_A,)).payloads[_A]
        )
        assert base.store is not None
        variants = [
            base,
            base.model_copy(update={"password": SecretStr(token_hex(16))}),
            base.model_copy(update={"user": "other"}),
            base.model_copy(
                update={
                    "store": base.store.model_copy(
                        update={"secret_access_key": SecretStr(token_hex(16))}
                    )
                }
            ),
            base.model_copy(
                update={"store": base.store.model_copy(update={"endpoint": "other.test"})}
            ),
        ]

        assert len({routed.credential_fingerprint(v) for v in variants}) == len(variants)

    def test_a_remapped_lake_changes_the_access_fingerprint(self) -> None:
        routed = _routed_ducklake(_DuckLakeSecrets(_A))

        assert routed.access_fingerprint_extra_parts(_A) == (
            "ducklake",
            "meta.test",
            "5432",
            "lakemeta",
            "t_aaaa",
            "/data/aaaa/",
        )

    async def test_a_failed_build_step_never_reports_the_password_or_the_store_key(
        self, fake_engine: type[_FakeEngine], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        secrets = _DuckLakeSecrets(_A, store=(_A,))
        payload = secrets.payloads[_A]
        hidden = (payload["password"], payload["store"]["secret_access_key"])

        async def failing(engine: _FakeEngine, command: str, *args: Any, **kwargs: Any) -> None:
            if command.startswith("ATTACH"):
                raise duckdb.IOException(f"could not connect: {hidden[0]} / {hidden[1]}")

        monkeypatch.setattr(_FakeEngine, "run_command", failing)
        routed = _routed_ducklake(secrets, catalog=lambda t: _ducklake(t, s3=True))
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value.code == "duckdb_session_setup_failed"
        assert ei.value.details["error"] == "could not connect: *** / ***"
        await routed.close()


class TestDuckLakeChecks:
    async def test_empty_role_credentials_are_refused_before_any_instance(
        self, fake_engine: type[_FakeEngine]
    ) -> None:
        """libpq would fall back to the OS user, or to PGPASSWORD and ~/.pgpass."""

        for field in ("user", "password"):
            secrets = _DuckLakeSecrets(_A)
            secrets.payloads[_A][field] = ""
            routed = _routed_ducklake(secrets)
            await routed.startup()

            with pytest.raises(CoreException) as ei:
                await _read(routed, _A)

            assert ei.value.code == "duckdb_lake_role_credentials_empty", field
            await routed.close()

        assert fake_engine.built == []

    async def test_a_local_lake_takes_no_ca_bundle(
        self, fake_engine: type[_FakeEngine], tmp_path: Any
    ) -> None:
        bundle = tmp_path / "ca.pem"
        bundle.write_text(_PEM)
        secrets = _DuckLakeSecrets(_A, _B, store=(_B,))
        routed = _routed_ducklake(
            secrets,
            catalog=lambda t: _ducklake(t, s3=t == _B),
            ca_cert_file=str(bundle),
        )
        await routed.startup()

        await _read(routed, _A)
        await _read(routed, _B)

        local, store = fake_engine.built
        assert not any("ca_cert_file" in s or str(bundle) in s for s in local.statements)
        assert f"SET GLOBAL allowed_paths=['{bundle.resolve()}']" in store.statements
        await routed.close()

    async def test_a_local_data_path_through_a_symlink_is_refused(
        self, fake_engine: type[_FakeEngine], tmp_path: Any
    ) -> None:
        real = tmp_path / "real"
        real.mkdir()
        (tmp_path / "link").symlink_to(real)
        routed = _routed_ducklake(
            _DuckLakeSecrets(_A),
            catalog=lambda t: _catalog_with(f"{tmp_path}/link/"),
        )
        await routed.startup()

        with pytest.raises(CoreException) as ei:
            await _read(routed, _A)

        assert ei.value.code == "duckdb_lake_catalog_invalid"
        assert fake_engine.built == []
        await routed.close()

    def test_an_iceberg_and_a_ducklake_principal_never_share_a_fingerprint(self) -> None:
        routed = _routed_ducklake(_DuckLakeSecrets(_A))
        secret = token_hex(16)
        iceberg = LakeCatalogCredentials(client_id="same", client_secret=SecretStr(secret))
        ducklake = DuckLakeCatalogCredentials(user="same", password=SecretStr(secret))

        assert routed.credential_fingerprint(iceberg) != routed.credential_fingerprint(ducklake)

    def test_a_local_data_path_is_probed_with_a_dot_segment_and_an_s3_one_is_not(self) -> None:
        local = routed_client._ducklake_outside(_ducklake(_A), None)
        store = routed_client._ducklake_outside(_ducklake(_A, s3=True), "/etc/ssl/ca.pem")

        assert "/data/aaaa/../forze-attest/x" in local
        assert "/data/aaaa-forze-attest/x" in local
        assert not any(".." in path for path in store)
        assert "/etc/ssl/forze-attest" in store

    @pytest.mark.parametrize(
        ("path", "grants", "problems"),
        [
            (
                "postgres:",
                {
                    "writable": 0,
                    "creates": False,
                    "super": False,
                    "others": 0,
                    "granted": 0,
                    "reaching": 0,
                },
                [],
            ),
            (
                "postgres:host=h password=p",
                {
                    "writable": 2,
                    "creates": True,
                    "super": True,
                    "others": 1,
                    "granted": 3,
                    "reaching": 1,
                },
                [
                    "lake attached with more than the metadata path",
                    "metadata role can write 2 tables",
                    "metadata role can create in its schema",
                    "metadata role is a superuser",
                    "metadata role can read 1 other lakes' metadata",
                    "metadata role can read 3 relations outside its schema",
                    "metadata schema holds 1 views reading outside it",
                ],
            ),
        ],
    )
    async def test_the_lake_path_and_the_roles_grants_are_read_back(
        self, path: str, grants: dict[str, Any], problems: list[str]
    ) -> None:
        asked: list[str] = []

        class _Engine:
            async def run_query(self, sql: str) -> Any:
                asked.append(sql)
                rows = [{"path": path}] if "duckdb_databases" in sql else [grants]
                return SimpleNamespace(rows=rows)

        catalog = DuckLakeTenantCatalog(
            host="h", database="d", metadata_schema="t'x", data_path="/data/a/"
        )

        assert await routed_client._ducklake_problems(_Engine(), catalog) == problems  # type: ignore[arg-type]
        # The schema reaches Postgres as a literal inside a DuckDB literal: quoted twice.
        assert "nspname = ''t''''x''" in asked[-1]
        assert asked[-1].startswith("SELECT * FROM postgres_query('__ducklake_metadata_lake', ")

    async def test_unreadable_grants_are_a_problem(self) -> None:
        class _Engine:
            async def run_query(self, sql: str) -> Any:
                if "postgres_query" in sql:
                    raise duckdb.CatalogException("postgres_query does not exist")
                return SimpleNamespace(rows=[{"path": "postgres:"}])

        problems = await routed_client._ducklake_problems(_Engine(), _ducklake(_A))  # type: ignore[arg-type]

        assert problems == ["metadata role's grants unreadable"]


def test_a_ducklake_session_attests_its_persistent_secrets_off_and_attached_paths() -> None:
    """D-100: ``ATTACH`` appends the metadata path, read as files in the working directory."""

    expected = routed_client._ducklake_expected_settings(
        _ducklake(_A),
        on_s3=False,
        working_directory="/srv",
        http_timeout_seconds=5,
        engine=DuckDbConfig(threads=None),
        extension_directory=None,
        ca_cert_file=None,
    )

    assert expected["allow_persistent_secrets"] is False
    assert expected["allowed_directories"] == ["/data/aaaa/"]
    assert expected["allowed_paths"] == [
        "/srv/postgres:",
        "/srv/postgres:.wal",
        "/srv/postgres:.wal.checkpoint",
        "/srv/postgres:.wal.recovery",
    ]
    assert "http_timeout" not in expected and "threads" not in expected


# ....................... #


def test_unset_engine_threads_and_extension_directory_are_not_attested() -> None:
    expected = routed_client._expected_settings(
        _catalog(_A),
        http_timeout_seconds=5,
        engine=DuckDbConfig(threads=None),
        extension_directory=None,
        ca_cert_file=None,
    )

    assert "threads" not in expected
    assert "extension_directory" not in expected


# ....................... #


@pytest.mark.parametrize("seed", range(20))
def test_the_pool_holds_under_perturbed_schedules(
    seed: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DST: eviction under a running statement, the guarded drain, and a rotate-evict race.

    Every statement asserts its instance stays open from start to finish, and returns the
    secret its instance was built with. After the run every instance ever built is closed.
    """

    _FakeEngine.built = []
    monkeypatch.setattr(routed_client, "DuckDbClient", _FakeEngine)
    monkeypatch.setattr(routed_client, "_attest", _attested)

    async def scenario() -> None:
        secrets = _MemSecrets(_A, _B, _C)
        routed = _routed(secrets, max_cached_tenants=2, max_concurrent_queries_total=3)
        await routed.startup()
        rng = random.Random(seed)

        async def rotate(tenant: UUID, secret: str) -> None:
            await asyncio.sleep(rng.random() / 10)
            secrets.rotate(tenant, secret)
            await routed.evict_tenant(tenant)

        reads = [
            asyncio.create_task(_read(routed, rng.choice((_A, _B, _C)), rng.random() / 10))
            for _ in range(24)
        ]
        rotated = {t: f"{rng.getrandbits(64):016x}" for t in (_A, _B)}
        rotations = [asyncio.create_task(rotate(t, rotated[t])) for t in (_A, _B)]
        known = {*secrets.first.values(), *rotated.values()}

        results = await asyncio.gather(*reads, *rotations)
        await routed.close()

        assert all(r[0]["secret"] in known for r in results[: len(reads)])
        assert all(engine.closed for engine in _FakeEngine.built)

    run_simulation(scenario, seed=seed, scheduler=PCTReorderer(rng=random.Random(seed)))
