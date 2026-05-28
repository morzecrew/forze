"""Unit tests for write conflict_target resolution."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze_postgres.kernel.sql.conflict_target import resolve_write_conflict_target
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector


@pytest.mark.asyncio
async def test_resolve_auto_returns_primary_key() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_primary_key_columns = AsyncMock(return_value=("tenant_id", "id"))

    out = await resolve_write_conflict_target(
        intro,
        schema="public",
        relation="docs",
        configured=None,
    )
    assert out == ("tenant_id", "id")


@pytest.mark.asyncio
async def test_resolve_auto_fails_when_pk_unmappable() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_primary_key_columns = AsyncMock(return_value=())

    with pytest.raises(CoreException, match="Cannot infer conflict_target"):
        await resolve_write_conflict_target(
            intro,
            schema="public",
            relation="docs",
            configured=None,
        )


@pytest.mark.asyncio
async def test_resolve_explicit_validates_constraint() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.constraint_exists_for_columns = AsyncMock(return_value=True)

    out = await resolve_write_conflict_target(
        intro,
        schema="public",
        relation="docs",
        configured=("id",),
    )
    assert out == ("id",)
    intro.constraint_exists_for_columns.assert_awaited_once_with(
        schema="public",
        relation="docs",
        columns=("id",),
    )


@pytest.mark.asyncio
async def test_resolve_explicit_invalid_constraint() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.constraint_exists_for_columns = AsyncMock(return_value=False)

    with pytest.raises(CoreException, match="does not match"):
        await resolve_write_conflict_target(
            intro,
            schema="public",
            relation="docs",
            configured=("slug",),
        )
