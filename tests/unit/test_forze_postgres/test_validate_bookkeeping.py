"""Unit tests for Postgres document bookkeeping validation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("psycopg")


from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.validation.validate_bookkeeping import (
    PostgresDocumentBookkeepingSpec,
    validate_postgres_document_bookkeeping,
)


@pytest.mark.asyncio
async def test_bookkeeping_database_strategy_requires_update_trigger() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_relation_update_triggers = AsyncMock(return_value=frozenset())

    with pytest.raises(CoreException, match="postgres_bookkeeping_validation_failed"):
        await validate_postgres_document_bookkeeping(
            intro,
            [
                PostgresDocumentBookkeepingSpec(
                    name="doc",
                    bookkeeping_strategy="database",
                    write_relation=("public", "projects"),
                ),
            ],
        )


@pytest.mark.asyncio
async def test_bookkeeping_database_strategy_passes_with_trigger() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_relation_update_triggers = AsyncMock(
        return_value=frozenset({"projects_bump_rev"}),
    )

    await validate_postgres_document_bookkeeping(
        intro,
        [
            PostgresDocumentBookkeepingSpec(
                name="doc",
                bookkeeping_strategy="database",
                write_relation=("public", "projects"),
            ),
        ],
    )


@pytest.mark.asyncio
async def test_bookkeeping_application_strategy_warns_on_triggers() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_relation_update_triggers = AsyncMock(
        return_value=frozenset({"projects_bump_rev"}),
    )

    await validate_postgres_document_bookkeeping(
        intro,
        [
            PostgresDocumentBookkeepingSpec(
                name="doc",
                bookkeeping_strategy="application",
                write_relation=("public", "projects"),
            ),
        ],
    )


# ....................... #

_LOGGER = "forze_postgres.kernel.catalog.validation.validate_bookkeeping.logger"


def _history_spec(strategy: str = "database") -> PostgresDocumentBookkeepingSpec:
    return PostgresDocumentBookkeepingSpec(
        name="doc",
        bookkeeping_strategy=strategy,  # type: ignore[arg-type]
        write_relation=("public", "projects"),
        history_enabled=True,
    )


def _intro(
    *,
    insert_triggers: frozenset[str],
    update_and_delete_row_triggers: frozenset[str] = frozenset(),
) -> MagicMock:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_relation_update_triggers = AsyncMock(
        return_value=frozenset({"projects_bump_rev"}),
    )
    intro.insert_triggers = AsyncMock(return_value=insert_triggers)
    intro.update_and_delete_row_triggers = AsyncMock(
        return_value=update_and_delete_row_triggers,
    )

    return intro


@pytest.mark.asyncio
async def test_bookkeeping_database_history_warns_without_insert_trigger() -> None:
    intro = _intro(insert_triggers=frozenset())

    with patch(_LOGGER) as mock_logger:
        await validate_postgres_document_bookkeeping(intro, [_history_spec()])

    intro.insert_triggers.assert_awaited_once_with(schema="public", relation="projects")
    intro.update_and_delete_row_triggers.assert_awaited_once_with(
        schema="public",
        relation="projects",
    )
    mock_logger.warning.assert_called_once()
    args = mock_logger.warning.call_args.args
    assert "no enabled trigger fires on every INSERT" in args[0]
    assert args[1:] == ("doc", "public", "projects")


@pytest.mark.asyncio
async def test_bookkeeping_database_history_quiet_with_insert_trigger() -> None:
    intro = _intro(insert_triggers=frozenset({"projects_history"}))

    with patch(_LOGGER) as mock_logger:
        await validate_postgres_document_bookkeeping(intro, [_history_spec()])

    mock_logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_bookkeeping_database_history_quiet_with_superseded_row_trigger() -> None:
    intro = _intro(
        insert_triggers=frozenset(),
        update_and_delete_row_triggers=frozenset({"projects_history"}),
    )

    with patch(_LOGGER) as mock_logger:
        await validate_postgres_document_bookkeeping(intro, [_history_spec()])

    mock_logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_bookkeeping_database_without_history_skips_insert_triggers() -> None:
    intro = _intro(insert_triggers=frozenset())
    spec = PostgresDocumentBookkeepingSpec(
        name="doc",
        bookkeeping_strategy="database",
        write_relation=("public", "projects"),
    )

    with patch(_LOGGER) as mock_logger:
        await validate_postgres_document_bookkeeping(intro, [spec])

    intro.insert_triggers.assert_not_awaited()
    mock_logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_bookkeeping_application_history_still_warns_on_update_triggers() -> None:
    intro = _intro(insert_triggers=frozenset())

    with patch(_LOGGER) as mock_logger:
        await validate_postgres_document_bookkeeping(intro, [_history_spec("application")])

    intro.insert_triggers.assert_not_awaited()
    mock_logger.warning.assert_called_once()
    assert "risk of double revision bumps" in mock_logger.warning.call_args.args[0]
