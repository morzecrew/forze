"""Regression tests for :class:`~forze_postgres.kernel.gateways.base.PostgresGateway` read metadata."""

from unittest.mock import MagicMock

from forze.domain.models import Document

from forze_postgres.kernel.gateways import PostgresGateway, PostgresQualifiedName
from forze_postgres.kernel.introspect import PostgresIntrospector


class _Doc(Document):
    sku: str


def test_read_fields_is_frozenset_and_stable() -> None:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.cache_partition_key = None
    gw = PostgresGateway(
        source_qname=PostgresQualifiedName("public", "items"),
        client=MagicMock(),
        model_type=_Doc,
        introspector=intro,
        tenant_aware=False,
    )

    assert isinstance(gw.read_fields, frozenset)
    assert gw.read_fields == gw.read_fields
    assert "id" in gw.read_fields
    assert "sku" in gw.read_fields
