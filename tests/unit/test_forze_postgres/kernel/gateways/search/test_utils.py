import io
import json
import logging

import structlog

from forze.application.contracts.search.internal.specs import (
    SearchFieldSpecInternal,
    SearchGroupSpecInternal,
    SearchIndexSpecInternal,
)
from forze.base.logging import configure_logging
from forze_postgres.kernel.gateways.search.utils import fts_map_groups


def _cleanup_postgres_logging() -> None:
    structlog.reset_defaults()
    lg = logging.getLogger("forze_postgres.kernel")
    lg.handlers.clear()
    lg.propagate = True


def test_fts_map_groups_truncation() -> None:
    buf = io.StringIO()
    try:
        configure_logging(
            level="warning",
            logger_names=["forze_postgres.kernel"],
            stream=buf,
            render_mode="json",
        )
        groups = [
            SearchGroupSpecInternal(name=f"group_{i}", weight=float(i))
            for i in range(5, 0, -1)
        ]
        fields = [SearchFieldSpecInternal(path="title", group="group_5")]
        spec = SearchIndexSpecInternal(fields=fields, groups=groups)

        result = fts_map_groups(spec)

        assert len(result) == 4
        assert "group_5" in result
        assert "group_4" in result
        assert "group_3" in result
        assert "group_2" in result
        assert "group_1" not in result

        records = [
            json.loads(line)
            for line in buf.getvalue().splitlines()
            if line.strip().startswith("{")
        ]
        warning_rows = [r for r in records if r.get("level") == "warning"]
        assert warning_rows
        event = warning_rows[-1]["event"]
        assert "Postgres only supports 4 weights" in event
    finally:
        _cleanup_postgres_logging()


def test_fts_map_groups_default() -> None:
    fields = [SearchFieldSpecInternal(path="title")]
    spec = SearchIndexSpecInternal(fields=fields, groups=[])

    result = fts_map_groups(spec)

    assert result == {"__default__": "A"}


def test_fts_map_groups_normal() -> None:
    groups = [
        SearchGroupSpecInternal(name="A_group", weight=10.0),
        SearchGroupSpecInternal(name="B_group", weight=5.0),
    ]
    fields = [SearchFieldSpecInternal(path="title", group="A_group")]
    spec = SearchIndexSpecInternal(fields=fields, groups=groups)

    result = fts_map_groups(spec)

    assert result == {"A_group": "A", "B_group": "B"}
