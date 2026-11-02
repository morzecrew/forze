"""Integration tests for Postgres bookkeeping validation against real triggers."""

from unittest.mock import patch
from uuid import uuid4

import pytest

from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_bookkeeping import (
    PostgresDocumentBookkeepingSpec,
    validate_postgres_document_bookkeeping,
)
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #

_LOGGER = "forze_postgres.kernel.catalog.validation.validate_bookkeeping.logger"


async def _setup(pg_client: PostgresClient, ddl: str) -> tuple[str, str]:
    """Run *ddl* with ``{t}`` as a fresh table name and ``{fn}`` as a no-op trigger function."""

    suf = uuid4().hex[:12]
    t = f"bk_{suf}"
    fn = f"bk_fn_{suf}"
    await pg_client.execute(
        f"CREATE FUNCTION {fn}() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ "
        "LANGUAGE plpgsql;"
    )
    await pg_client.execute(ddl.format(t=t, fn=fn))

    return t, fn


async def _validate(
    pg_client: PostgresClient,
    t: str,
    *,
    history_enabled: bool = False,
) -> list[tuple[object, ...]]:
    """Validate *t* on the ``database`` strategy; return the warnings logged."""

    spec = PostgresDocumentBookkeepingSpec(
        name="doc",
        bookkeeping_strategy="database",
        write_relation=("public", t),
        history_enabled=history_enabled,
    )

    with patch(_LOGGER) as mock_logger:
        await validate_postgres_document_bookkeeping(
            PostgresIntrospector(client=pg_client),
            [spec],
        )

    return [c.args for c in mock_logger.warning.call_args_list]


# ....................... #


@pytest.mark.parametrize(
    ("clause", "passes"),
    [
        ("DISABLE TRIGGER", False),
        ("ENABLE REPLICA TRIGGER", False),
        ("ENABLE ALWAYS TRIGGER", True),
    ],
)
async def test_update_trigger_counts_only_when_it_fires_in_a_normal_session(
    pg_client: PostgresClient,
    clause: str,
    passes: bool,
) -> None:
    t, _ = await _setup(
        pg_client,
        """
        CREATE TABLE {t} (id int PRIMARY KEY, rev int NOT NULL);
        CREATE TRIGGER {t}_bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        """,
    )
    await pg_client.execute(f"ALTER TABLE {t} {clause} {t}_bump")

    if passes:
        await _validate(pg_client, t)

    else:
        with pytest.raises(CoreException, match="postgres_bookkeeping_validation_failed"):
            await _validate(pg_client, t)


async def test_disabled_insert_trigger_keeps_the_history_warning(
    pg_client: PostgresClient,
) -> None:
    t, _ = await _setup(
        pg_client,
        """
        CREATE TABLE {t} (id int PRIMARY KEY, rev int NOT NULL);
        CREATE TRIGGER {t}_bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        CREATE TRIGGER {t}_hist AFTER INSERT ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        ALTER TABLE {t} DISABLE TRIGGER {t}_hist;
        """,
    )

    assert len(await _validate(pg_client, t, history_enabled=True)) == 1

    await pg_client.execute(f"ALTER TABLE {t} ENABLE ALWAYS TRIGGER {t}_hist")

    assert await _validate(pg_client, t, history_enabled=True) == []


@pytest.mark.parametrize(
    ("hist", "quiet"),
    [
        ("BEFORE UPDATE OR DELETE ON {t} FOR EACH ROW", True),
        ("AFTER UPDATE OR DELETE ON {t} FOR EACH STATEMENT", False),
        ("AFTER DELETE ON {t} FOR EACH ROW", False),
    ],
)
async def test_superseded_row_history_trigger_silences_the_history_warning(
    pg_client: PostgresClient,
    hist: str,
    quiet: bool,
) -> None:
    t, fn = await _setup(
        pg_client,
        """
        CREATE TABLE {t} (id int PRIMARY KEY, rev int NOT NULL);
        CREATE TRIGGER {t}_bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        """,
    )
    await pg_client.execute(f"CREATE TRIGGER {t}_hist {hist.format(t=t)} EXECUTE FUNCTION {fn}();")

    assert len(await _validate(pg_client, t, history_enabled=True)) == (0 if quiet else 1)


# ....................... #

_PARTITIONED = """
CREATE TABLE {t} (id int NOT NULL, k int NOT NULL, rev int NOT NULL) PARTITION BY LIST (k);
CREATE TABLE {t}_a PARTITION OF {t} FOR VALUES IN (1);
CREATE TABLE {t}_b PARTITION OF {t} FOR VALUES IN (2) PARTITION BY LIST (id);
CREATE TABLE {t}_b1 PARTITION OF {t}_b FOR VALUES IN (1);
"""
"""A partitioned table whose leaves are ``{t}_a`` and ``{t}_b1``, one level deeper."""


async def test_partitioned_parent_update_trigger_passes(pg_client: PostgresClient) -> None:
    t, _ = await _setup(
        pg_client,
        _PARTITIONED
        + "CREATE TRIGGER bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();",
    )

    await _validate(pg_client, t)


async def test_partitioned_update_trigger_on_every_leaf_passes(
    pg_client: PostgresClient,
) -> None:
    t, fn = await _setup(pg_client, _PARTITIONED)
    await pg_client.execute(
        f"CREATE TRIGGER bump BEFORE UPDATE ON {t}_a FOR EACH ROW EXECUTE FUNCTION {fn}();"
    )

    with pytest.raises(CoreException, match="postgres_bookkeeping_validation_failed"):
        await _validate(pg_client, t)

    await pg_client.execute(
        f"CREATE TRIGGER bump BEFORE UPDATE ON {t}_b1 FOR EACH ROW EXECUTE FUNCTION {fn}();"
    )

    await _validate(pg_client, t)


async def test_partitioned_parent_trigger_disabled_on_one_leaf_refuses(
    pg_client: PostgresClient,
) -> None:
    t, _ = await _setup(
        pg_client,
        _PARTITIONED
        + """
        CREATE TRIGGER bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        ALTER TABLE {t}_b1 DISABLE TRIGGER bump;
        """,
    )

    with pytest.raises(CoreException, match="postgres_bookkeeping_validation_failed"):
        await _validate(pg_client, t)


async def test_partitioned_history_needs_an_insert_trigger_on_every_leaf(
    pg_client: PostgresClient,
) -> None:
    t, fn = await _setup(
        pg_client,
        _PARTITIONED
        + """
        CREATE TRIGGER bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        CREATE TRIGGER hist AFTER INSERT ON {t}_a FOR EACH ROW EXECUTE FUNCTION {fn}();
        """,
    )

    assert len(await _validate(pg_client, t, history_enabled=True)) == 1

    await pg_client.execute(
        f"CREATE TRIGGER hist AFTER INSERT ON {t}_b1 FOR EACH ROW EXECUTE FUNCTION {fn}();"
    )

    assert await _validate(pg_client, t, history_enabled=True) == []


async def test_partitioned_history_needs_a_superseded_row_trigger_on_every_leaf(
    pg_client: PostgresClient,
) -> None:
    t, fn = await _setup(
        pg_client,
        _PARTITIONED
        + """
        CREATE TRIGGER bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        CREATE TRIGGER hist BEFORE UPDATE OR DELETE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        """,
    )

    assert await _validate(pg_client, t, history_enabled=True) == []

    await pg_client.execute(f"ALTER TABLE {t}_b1 DISABLE TRIGGER hist")

    assert len(await _validate(pg_client, t, history_enabled=True)) == 1


async def test_partitioned_parent_statement_insert_trigger_counts(
    pg_client: PostgresClient,
) -> None:
    t, _ = await _setup(
        pg_client,
        _PARTITIONED
        + """
        CREATE TRIGGER bump BEFORE UPDATE ON {t} FOR EACH ROW EXECUTE FUNCTION {fn}();
        CREATE TRIGGER hist AFTER INSERT ON {t} FOR EACH STATEMENT EXECUTE FUNCTION {fn}();
        """,
    )

    assert await _validate(pg_client, t, history_enabled=True) == []
