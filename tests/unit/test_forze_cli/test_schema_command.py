"""``forze schema``: loading ``--deps``, the DSN rule and the exit-code contract.

The catalog read itself needs Postgres and is covered by
``tests/integration/test_forze_postgres/test_pg_owned_schema_diff.py``. Here it is replaced by a
recorded result, so each exit code is reached without a database.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest

pytest.importorskip("typer")

from typer.testing import CliRunner

from forze.base.exceptions import CoreException, exc
from forze_cli import schema as schema_cli
from forze_cli.app import app
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    CatalogRelation,
    RelationDelta,
    diff_relation,
)
from forze_postgres.kernel.client.errors import PRIVILEGE_DENIED_CODE

pytestmark = pytest.mark.unit

_DEPS = "tests.support.schema_snapshots:deps"

runner = CliRunner()


def _patch_deltas(
    monkeypatch: pytest.MonkeyPatch, catalog: CatalogRelation | Exception | None
) -> None:
    async def read(
        dsn: str, relations: Sequence[BoundOwnedRelation], *, keep_retired: bool = False
    ) -> list[RelationDelta]:
        assert dsn == "postgresql://reader@db/app"

        if isinstance(catalog, Exception):
            raise catalog

        return [
            diff_relation(relation, catalog, keep_retired=keep_retired) for relation in relations
        ]

    monkeypatch.setattr(schema_cli, "_read_deltas", read)
    monkeypatch.setenv(schema_cli.DSN_ENV, "postgresql://reader@db/app")


class TestSql:
    def test_it_prints_every_owned_table_the_deps_module_binds(self) -> None:
        result = runner.invoke(app, ["schema", "sql", "--deps", _DEPS])

        assert result.exit_code == 0, result.output
        # Twelve tables, and the delivery log's DEFAULT partition.
        assert result.stdout.count("CREATE TABLE") == 13
        assert "CREATE INDEX ON public.durable_run (status, name, created_at);" in result.stdout

    def test_a_sequence_holding_one_deps_module_is_accepted(self) -> None:
        result = runner.invoke(
            app, ["schema", "sql", "--deps", "tests.support.schema_cli:in_a_list"]
        )

        assert result.exit_code == 0, result.output

    @pytest.mark.parametrize(
        ("ref", "message"),
        [
            ("tests.support.schema_cli:two", "holds 2 PostgresDepsModule"),
            ("tests.support.schema_cli:not_deps", "is not a PostgresDepsModule"),
            ("tests.support.schema_cli:missing", "Cannot load --deps"),
            ("no_such_module_anywhere:deps", "Cannot load --deps"),
            ("no-colon", "Cannot load --deps"),
        ],
    )
    def test_a_deps_reference_that_names_no_single_module_is_a_configuration_error(
        self, ref: str, message: str
    ) -> None:
        result = runner.invoke(app, ["schema", "sql", "--deps", ref])

        assert result.exit_code == schema_cli.EXIT_CONFIG
        assert message in result.stderr

    def test_a_tenant_scoped_relation_is_skipped_with_a_warning(self) -> None:
        result = runner.invoke(
            app, ["schema", "sql", "--deps", "tests.support.schema_cli:tenant_scoped"]
        )

        assert result.exit_code == 0
        assert "skipped durable_run" in result.stderr
        assert result.stdout.count("CREATE TABLE") == 1

    def test_a_tenant_binds_the_relation_its_resolver_names(self) -> None:
        tenant = "6f0c1f5e-8a7b-4e0e-9a6b-0d3f6b1c2a11"

        result = runner.invoke(
            app,
            [
                "schema", "sql", "--deps", "tests.support.schema_cli:tenant_scoped",
                "--tenant", tenant, "--tenant", tenant,
            ],
        )

        assert result.exit_code == 0, result.output
        assert "skipped" not in result.stderr
        assert "CREATE TABLE t.durable_run (" in result.stdout
        # The tenant given twice binds its table once.
        assert result.stdout.count("CREATE TABLE") == 2

    @pytest.mark.parametrize("specs", ["specs", "frozen_specs"])
    def test_specs_adds_the_framework_document_tables(self, specs: str) -> None:
        deps = "tests.support.schema_cli:with_documents"
        result = runner.invoke(
            app,
            ["schema", "sql", "--deps", deps, "--specs", f"tests.support.schema_cli:{specs}"],
        )

        assert result.exit_code == 0, result.output
        # The store table first, then the document tables in route order.
        assert [line for line in result.output.splitlines() if line.startswith("CREATE TABLE")] == [
            "CREATE TABLE public.durable_run (",
            "CREATE TABLE public.authn_password_accounts (",
            "CREATE TABLE public.jobs (",
        ]
        assert "CREATE UNIQUE INDEX ON public.authn_password_accounts (username);" in result.output

    def test_without_specs_only_the_store_tables_are_printed(self) -> None:
        result = runner.invoke(
            app, ["schema", "sql", "--deps", "tests.support.schema_cli:with_documents"]
        )

        assert result.exit_code == 0, result.output
        assert "public.jobs" not in result.output

    @pytest.mark.parametrize(
        ("ref", "message"),
        [
            ("tests.support.schema_cli:not_deps", "is not a SpecRegistry"),
            ("tests.support.schema_cli:missing", "Cannot load --specs"),
        ],
    )
    def test_a_specs_reference_that_names_no_registry_is_a_configuration_error(
        self, ref: str, message: str
    ) -> None:
        result = runner.invoke(app, ["schema", "sql", "--deps", _DEPS, "--specs", ref])

        assert result.exit_code == schema_cli.EXIT_CONFIG
        assert message in result.output

    def test_a_tenant_that_is_not_a_uuid_is_a_usage_error(self) -> None:
        result = runner.invoke(
            app,
            ["schema", "sql", "--deps", "tests.support.schema_cli:tenant_scoped", "--tenant", "acme"],
        )

        assert result.exit_code == schema_cli.EXIT_USAGE
        assert "is not a tenant id" in result.stderr


class TestDiff:
    def test_the_dsn_is_refused_as_a_flag(self) -> None:
        result = runner.invoke(
            app, ["schema", "diff", "--deps", _DEPS, "--dsn", "postgresql://u:secret@db/app"]
        )

        assert result.exit_code == schema_cli.EXIT_USAGE
        assert schema_cli.DSN_ENV in result.stderr
        assert "secret" not in result.output

    def test_an_unset_dsn_is_a_configuration_error_that_names_the_role_rule(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(schema_cli.DSN_ENV, raising=False)

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS])

        assert result.exit_code == schema_cli.EXIT_CONFIG
        assert "read-only role" in result.stderr

    def test_the_help_states_the_read_only_role_requirement(self) -> None:
        result = runner.invoke(app, ["schema", "diff", "--help"])

        assert "read-only role" in " ".join(result.stdout.split())
        assert "--dsn" not in result.stdout

    def test_a_missing_table_prints_its_create_and_exits_10(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_deltas(monkeypatch, None)

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS])

        assert result.exit_code == schema_cli.EXIT_DELTA
        # Twelve tables, and the delivery log's DEFAULT partition.
        assert result.stdout.count("CREATE TABLE") == 13

    def test_a_refusal_exits_11(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_deltas(monkeypatch, CatalogRelation(kind="view"))

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS])

        assert result.exit_code == schema_cli.EXIT_REFUSAL
        assert "REFUSED" in result.stdout

    def test_check_prints_nothing_and_keeps_the_exit_code(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_deltas(monkeypatch, None)

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS, "--check"])

        assert result.exit_code == schema_cli.EXIT_DELTA
        assert result.stdout == ""

    @pytest.mark.parametrize(
        ("error", "code"),
        [
            (CoreException.throttled("Timed out acquiring a database connection."), 75),
            (CoreException.concurrency("Transient database connectivity issue."), 75),
            (CoreException.infrastructure("Database unavailable."), 75),
            (CoreException.authorization("Permission denied."), 78),
            (CoreException.internal("Relation has no columns."), 1),
            (CoreException.internal("Denied.", code=PRIVILEGE_DENIED_CODE), 78),
            (CoreException.configuration("Cache partition required."), 78),
        ],
    )
    def test_a_failed_catalog_read_maps_to_its_exit_code(
        self, monkeypatch: pytest.MonkeyPatch, error: CoreException, code: int
    ) -> None:
        _patch_deltas(monkeypatch, error)

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS])

        assert result.exit_code == code
        assert "Cannot read the catalog" in result.stderr

    @pytest.mark.parametrize(
        ("dsn", "code"),
        [
            ("host=db password=hunter2secret bogus", 78),
            ("postgresql://reader:hunter2secret@127.0.0.1:1/app", 75),
        ],
        ids=["unparsable", "refused"],
    )
    def test_a_bad_dsn_fails_at_once_without_echoing_the_password(
        self, monkeypatch: pytest.MonkeyPatch, dsn: str, code: int
    ) -> None:
        monkeypatch.setenv(schema_cli.DSN_ENV, dsn)

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS])

        assert result.exit_code == code
        assert schema_cli.DSN_ENV in result.stderr
        assert "hunter2secret" not in result.output

    @pytest.mark.parametrize(
        ("message", "code"),
        [
            ('FATAL:  password authentication failed for user "reader"', 78),
            ('FATAL:  database "app" does not exist', 78),
            ('FATAL:  no pg_hba.conf entry for host "10.0.0.1", user "reader"', 78),
            ("FATAL:  sorry, too many clients already", 75),
            ("FATAL:  the database system is starting up", 75),
        ],
    )
    def test_a_refused_login_is_configuration_and_a_busy_server_is_retryable(
        self, monkeypatch: pytest.MonkeyPatch, message: str, code: int
    ) -> None:
        import psycopg

        async def connect(*args: object, **kwargs: object) -> None:
            raise psycopg.OperationalError(f"connection failed: {message}")

        monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect)
        monkeypatch.setenv(schema_cli.DSN_ENV, "postgresql://reader@db/app")

        result = runner.invoke(app, ["schema", "diff", "--deps", _DEPS])

        assert result.exit_code == code
        assert message in result.stderr

    def test_a_skipped_relation_is_never_reported_in_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_deltas(monkeypatch, None)

        async def nothing(
            dsn: str, relations: Sequence[BoundOwnedRelation], *, keep_retired: bool = False
        ) -> list[RelationDelta]:
            return []

        monkeypatch.setattr(schema_cli, "_read_deltas", nothing)

        result = runner.invoke(
            app, ["schema", "diff", "--deps", "tests.support.schema_cli:tenant_scoped", "--check"]
        )

        assert result.exit_code == schema_cli.EXIT_REFUSAL
        assert "skipped durable_run" in result.stderr

    def test_a_deps_module_that_refuses_its_configuration_on_import_exits_78(self) -> None:
        result = runner.invoke(
            app, ["schema", "sql", "--deps", "tests.support.schema_cli_broken:deps"]
        )

        assert result.exit_code == schema_cli.EXIT_CONFIG
        assert "Cannot load --deps" in result.stderr

    def test_a_spec_registry_that_refuses_its_configuration_on_import_exits_78(self) -> None:
        result = runner.invoke(
            app,
            ["schema", "sql", "--deps", _DEPS, "--specs", "tests.support.schema_cli_broken:deps"],
        )

        assert result.exit_code == schema_cli.EXIT_CONFIG
        assert "Cannot load --specs" in result.stderr

    @pytest.mark.parametrize("command", ["sql", "diff"])
    def test_two_stores_on_one_table_are_a_configuration_error(
        self, command: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_deltas(monkeypatch, None)

        result = runner.invoke(
            app, ["schema", command, "--deps", "tests.support.schema_cli:shared_by_two_stores"]
        )

        assert result.exit_code == schema_cli.EXIT_CONFIG
        assert "share one owned table" in result.stderr

    def test_an_error_binding_that_is_not_configuration_is_not_reported_as_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import forze_postgres

        def broken(deps: object) -> None:
            raise exc.internal("binding broke")

        monkeypatch.setattr(forze_postgres, "owned_relations", broken)

        result = runner.invoke(app, ["schema", "sql", "--deps", _DEPS])

        assert result.exit_code == schema_cli.EXIT_INTERNAL
        assert "Cannot bind" not in result.stderr


@pytest.mark.parametrize(("platform", "selector"), [("win32", True), ("linux", False)])
def test_the_diff_runs_on_a_loop_psycopg_accepts(
    monkeypatch: pytest.MonkeyPatch, platform: str, selector: bool
) -> None:
    # psycopg refuses Windows' default proactor loop.
    import asyncio
    import sys

    seen: dict[str, object] = {}

    def run(coroutine: object, **kwargs: object) -> str:
        coroutine.close()  # type: ignore[attr-defined]
        seen.update(kwargs)
        return "ran"

    async def work() -> str:
        return "unused"

    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(asyncio, "run", run)

    assert schema_cli._run(work()) == "ran"
    assert (seen.get("loop_factory") is asyncio.SelectorEventLoop) is selector


def test_a_bug_in_the_deps_module_keeps_its_traceback() -> None:
    from forze.base.exceptions import CoreException

    result = runner.invoke(app, ["schema", "sql", "--deps", "tests.support.schema_cli_crash:deps"])

    assert result.exit_code == 1
    assert isinstance(result.exception, CoreException)
