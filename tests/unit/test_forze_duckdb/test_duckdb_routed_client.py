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
from datetime import timedelta
from secrets import token_hex
from typing import Any, ClassVar
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import duckdb
import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_dst.runtime import run_simulation
from forze_dst.scheduler import PCTReorderer
from forze_duckdb import DuckDbConfig, LakeTenantCatalog, RoutedDuckDbClient
from forze_duckdb.adapters import dynamic_read_error
from forze_duckdb.kernel.client import routed_client

# ----------------------- #

_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_C = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
_TIMEOUT = timedelta(seconds=5)

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

    async def run_command(self, command: str, *args: Any, **kwargs: Any) -> None:
        if command.startswith("CREATE SECRET"):
            self.secret = command.split("CLIENT_SECRET '", 1)[1].split("'", 1)[0]
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
    yield _FakeEngine


async def _attested(client: Any, expected: dict[str, str]) -> list[str]:
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


# ....................... #


class TestInstances:
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
        async def unlocked(client: Any, expected: dict[str, str]) -> list[str]:
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


class _Probe:
    """Answers the attestation's filesystem read with *error*, or with rows when it is None."""

    def __init__(self, error: Exception | None) -> None:
        self.error = error

    async def run_query(self, sql: str) -> None:
        if self.error is not None:
            raise self.error


@pytest.mark.parametrize(
    ("error", "disabled"),
    [
        (duckdb.PermissionException("LocalFileSystem has been disabled by configuration"), True),
        (duckdb.PermissionException("enable_external_access is disabled"), False),
        (duckdb.IOException("No files found that match the pattern"), False),
        (None, False),
    ],
    ids=["disabled", "other_refusal", "io_error", "read_succeeded"],
)
async def test_a_filesystem_attests_disabled_only_on_its_own_refusal(
    error: Exception | None, disabled: bool
) -> None:
    probe: Any = _Probe(error)

    assert await routed_client._filesystem_disabled(probe, "LocalFileSystem", "/x") is disabled


def test_unset_engine_threads_and_extension_directory_are_not_attested() -> None:
    expected = routed_client._expected_settings(
        _catalog(_A),
        http_timeout_seconds=5,
        engine=DuckDbConfig(threads=None),
        extension_directory=None,
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
