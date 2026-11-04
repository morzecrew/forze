"""Unit tests for how a set-based upsert binds its id arrays in
``forze_postgres.kernel.gateways.write``."""

import pytest

from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.catalog.introspect import PostgresType
from forze_postgres.kernel.gateways import PostgresWriteGateway

_placeholder = PostgresWriteGateway._PostgresWriteGateway__ids_placeholder  # type: ignore[attr-defined]


def test_an_id_array_goes_in_binary_for_a_uuid_id_column() -> None:
    types = {ID_FIELD: PostgresType(base="uuid", is_array=False, not_null=True)}

    assert _placeholder(types).as_string(None) == "%b"


@pytest.mark.parametrize(
    "types",
    [
        {ID_FIELD: PostgresType(base="text", is_array=False, not_null=True)},
        {ID_FIELD: PostgresType(base="uuid", is_array=True, not_null=True)},
        {},
    ],
)
def test_an_id_array_binds_as_before_for_any_other_id_column(
    types: dict[str, PostgresType],
) -> None:
    assert _placeholder(types).as_string(None) == "%s"
