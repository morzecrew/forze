"""Unit tests for Postgres document bookkeeping validation."""

from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("psycopg")


from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.validate_bookkeeping import (
    PostgresDocumentBookkeepingSpec,
    validate_postgres_document_bookkeeping,
)


@pytest.mark.asyncio
async def test_bookkeeping_database_strategy_requires_update_trigger() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_relation_update_triggers = AsyncMock(return_value=frozenset())

    with pytest.raises(exc.internal, match="postgres_bookkeeping_validation_failed"):
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
