
import logging
import pytest
from forze.application.contracts.search.internal import (
    SearchIndexSpecInternal,
    SearchGroupSpecInternal,
    SearchFieldSpecInternal,
)
from forze_postgres.kernel.gateways.search.utils import fts_map_groups

def test_fts_map_groups_truncation(caplog):
    # Setup: Create a SearchIndexSpecInternal with 5 groups
    groups = [
        SearchGroupSpecInternal(name=f"group_{i}", weight=float(i))
        for i in range(5, 0, -1)
    ]
    fields = [SearchFieldSpecInternal(path="title", group="group_5")]
    spec = SearchIndexSpecInternal(fields=fields, groups=groups)

    # Execute
    with caplog.at_level(logging.WARNING):
        result = fts_map_groups(spec)

    # Verify: Should only have 4 groups mapped
    assert len(result) == 4
    assert "group_5" in result
    assert "group_4" in result
    assert "group_3" in result
    assert "group_2" in result
    assert "group_1" not in result

    # Verify warning
    assert any(
        record.levelname == "WARNING"
        and "Postgres only supports 4 weights" in record.message
        for record in caplog.records
    )

def test_fts_map_groups_default():
    # Setup: Create a SearchIndexSpecInternal with no groups
    fields = [SearchFieldSpecInternal(path="title")]
    spec = SearchIndexSpecInternal(fields=fields, groups=[])

    # Execute
    result = fts_map_groups(spec)

    # Verify
    assert result == {"__default__": "A"}

def test_fts_map_groups_normal():
    # Setup: Create a SearchIndexSpecInternal with 2 groups
    groups = [
        SearchGroupSpecInternal(name="A_group", weight=10.0),
        SearchGroupSpecInternal(name="B_group", weight=5.0),
    ]
    fields = [SearchFieldSpecInternal(path="title", group="A_group")]
    spec = SearchIndexSpecInternal(fields=fields, groups=groups)

    # Execute
    result = fts_map_groups(spec)

    # Verify
    assert result == {"A_group": "A", "B_group": "B"}
