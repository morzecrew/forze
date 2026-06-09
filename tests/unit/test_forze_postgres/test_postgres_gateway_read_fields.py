"""Regression tests for :class:`~forze_postgres.kernel.gateways.base.PostgresGateway` read metadata."""

from unittest.mock import MagicMock

from forze.domain.models import Document

from forze_postgres.kernel.gateways import PostgresGateway, PostgresQualifiedName
from tests.unit._gateway_codec_helpers import codec_for
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector


class _Doc(Document):
    sku: str


def _gateway() -> PostgresGateway[_Doc]:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.cache_partition_key = None
    return PostgresGateway(
        relation=("public", "items"),
        client=MagicMock(),
        model_type=_Doc,
        codec=codec_for(_Doc),
        introspector=intro,
        tenant_aware=False,
    )


def test_read_fields_is_frozenset_and_stable() -> None:
    gw = _gateway()

    assert isinstance(gw.read_fields, frozenset)
    assert gw.read_fields == gw.read_fields
    assert "id" in gw.read_fields
    assert "sku" in gw.read_fields


class TestReturnClauseCache:
    def test_default_clause_is_memoized_per_alias(self) -> None:
        gw = _gateway()

        first = gw.return_clause()
        second = gw.return_clause()
        aliased = gw.return_clause(table_alias="v")

        # Same (return_type, alias) returns the cached composable instance.
        assert first is second
        # A different alias is a distinct cache entry, not the same object.
        assert aliased is not first
        assert gw.return_clause(table_alias="v") is aliased

    def test_return_type_clause_is_memoized(self) -> None:
        gw = _gateway()

        first = gw.return_clause(return_type=_Doc)
        second = gw.return_clause(return_type=_Doc)

        assert first is second

    def test_explicit_fields_are_not_cached(self) -> None:
        gw = _gateway()

        first = gw.return_clause(return_fields=["id", "sku"])
        second = gw.return_clause(return_fields=["id", "sku"])

        # Explicit projections are built each call (left uncached, bounded memo).
        assert first is not second

    def test_cached_clause_renders_expected_columns(self) -> None:
        gw = _gateway()

        rendered = gw.return_clause().as_string()

        assert '"id"' in rendered
        assert '"sku"' in rendered
