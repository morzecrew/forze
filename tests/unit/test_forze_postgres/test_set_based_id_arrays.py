"""Unit tests for how the Postgres gateways bind an array of document ids
(``forze_postgres.kernel.gateways.base.id_array``)."""

import pytest

from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.catalog.introspect import PostgresType
from forze_postgres.kernel.gateways.base import id_array


def test_an_id_array_goes_in_binary_for_a_uuid_id_column() -> None:
    types = {ID_FIELD: PostgresType(base="uuid", is_array=False, not_null=True)}

    assert id_array(types).as_string(None) == "%b::uuid[]"


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
    assert id_array(types).as_string(None) == "%s"
