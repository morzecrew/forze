"""The durable schema page, the adapter docstrings and the integration tests' DDL agree.

Applications copy their migrations from ``pages/docs/reference/durable-schema.md``, the
adapters document the same schema on their classes, and the Postgres integration tests create
it from ``tests/support/durable_schema.py``. Three copies drift unless something compares
them: a column added to one and not the others would pass every test the adapters have.
"""

import re
from pathlib import Path
from typing import Final

import pytest

from forze_mongo.adapters.durable import (
    MongoDurableFunctionStepAdapter,
    MongoDurableRunStore,
    MongoDurableScheduleStore,
)
from forze_postgres.adapters.durable import (
    PostgresDurableFunctionStepAdapter,
    PostgresDurableRunStore,
    PostgresDurableScheduleStore,
)
from tests.support.durable_schema import (
    DURABLE_RUN_COALESCE_INDEX,
    DURABLE_RUN_COLUMNS,
    DURABLE_SCHEDULE_COLUMNS,
    DURABLE_STEP_COLUMNS,
)

# ----------------------- #

_PAGE: Final[Path] = (
    Path(__file__).resolve().parents[2] / "pages" / "docs" / "reference" / "durable-schema.md"
)

_TABLE: Final[re.Pattern[str]] = re.compile(r"CREATE TABLE (\S+) \((.*?)\n\s*\);?", re.DOTALL)
_INDEX: Final[re.Pattern[str]] = re.compile(r"CREATE INDEX ON (\S+) (\(.*?\)(?: WHERE \w+)?);")
_MONGO_INDEX: Final[re.Pattern[str]] = re.compile(r"db\.(\S+?)\.createIndex\(([^()]*)\)")
_UNIQUE_INDEX: Final[re.Pattern[str]] = re.compile(r"CREATE UNIQUE INDEX ON (\S+) (.*?);")


def _words(text: str) -> str:
    return " ".join(text.split())


def _tables(text: str) -> dict[str, str]:
    return {name: _words(body) for name, body in _TABLE.findall(text)}


def _indexes(text: str) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}

    for name, columns in _INDEX.findall(text):
        found.setdefault(name, []).append(_words(columns))

    return found


def _mongo_indexes(text: str) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}

    for name, arguments in _MONGO_INDEX.findall(text):
        found.setdefault(name, []).append(re.sub(r"\s+|,(?=\s*$)", "", arguments))

    return found


def _doc(adapter: type) -> str:
    assert adapter.__doc__ is not None
    return adapter.__doc__


_PAGE_TEXT: Final[str] = _PAGE.read_text(encoding="utf-8")

_SOURCES: Final[dict[str, str]] = {
    "page": _PAGE_TEXT,
    **{
        adapter.__name__: _doc(adapter)
        for adapter in (
            PostgresDurableRunStore,
            PostgresDurableFunctionStepAdapter,
            PostgresDurableScheduleStore,
            MongoDurableRunStore,
            MongoDurableFunctionStepAdapter,
            MongoDurableScheduleStore,
        )
    },
}
"""Every text the comparisons below read."""

# ....................... #


@pytest.mark.parametrize(
    ("relation", "adapter", "columns"),
    [
        ("public.durable_run", PostgresDurableRunStore, DURABLE_RUN_COLUMNS),
        ("public.durable_step", PostgresDurableFunctionStepAdapter, DURABLE_STEP_COLUMNS),
        ("public.durable_schedule", PostgresDurableScheduleStore, DURABLE_SCHEDULE_COLUMNS),
    ],
    ids=["run", "step", "schedule"],
)
def test_each_postgres_table_reads_the_same_on_the_page_the_adapter_and_the_tests(
    relation: str, adapter: type, columns: str
) -> None:
    page = _tables(_PAGE_TEXT)[relation]

    assert _tables(_doc(adapter))["<relation>"] == page
    assert _words(columns) == page


@pytest.mark.parametrize(
    ("relation", "adapter"),
    [
        ("public.durable_run", PostgresDurableRunStore),
        ("public.durable_schedule", PostgresDurableScheduleStore),
    ],
)
def test_the_page_recommends_the_postgres_indexes_the_adapter_documents(
    relation: str, adapter: type
) -> None:
    assert _indexes(_PAGE_TEXT)[relation] == _indexes(_doc(adapter))["<relation>"]


@pytest.mark.parametrize(
    ("collection", "adapter"),
    [
        ("durable_run", MongoDurableRunStore),
        ("durable_step", MongoDurableFunctionStepAdapter),
        ("durable_schedule", MongoDurableScheduleStore),
    ],
)
def test_the_page_lists_the_mongo_indexes_the_adapter_documents(
    collection: str, adapter: type
) -> None:
    assert _mongo_indexes(_PAGE_TEXT)[collection] == _mongo_indexes(_doc(adapter))["<collection>"]


def test_the_page_documents_every_postgres_table_it_names() -> None:
    assert set(_tables(_PAGE_TEXT)) == {
        "public.durable_run",
        "public.durable_step",
        "public.durable_schedule",
    }
    assert set(_indexes(_PAGE_TEXT)) == {"public.durable_run", "public.durable_schedule"}
    assert set(_mongo_indexes(_PAGE_TEXT)) == {"durable_run", "durable_step", "durable_schedule"}


@pytest.mark.parametrize("source", sorted(_SOURCES))
def test_every_statement_in_every_source_is_one_the_comparisons_read(source: str) -> None:
    # A statement the patterns cannot read (named, CONCURRENTLY, split over lines) would
    # otherwise drop out of both sides of a comparison and pass unseen.
    text = _SOURCES[source]

    assert text.count("CREATE TABLE") == len(_TABLE.findall(text))
    assert text.count("CREATE INDEX") == len(_INDEX.findall(text))
    assert text.count("createIndex") == len(_MONGO_INDEX.findall(text))


def test_the_coalesce_index_reads_the_same_on_the_page_the_adapter_and_the_tests() -> None:
    page = dict(_UNIQUE_INDEX.findall(_PAGE_TEXT))
    adapter = dict(_UNIQUE_INDEX.findall(_doc(PostgresDurableRunStore)))
    tests = dict(_UNIQUE_INDEX.findall(DURABLE_RUN_COALESCE_INDEX + ";"))

    assert page["public.durable_run"] == adapter["<relation>"] == tests["{table}"]
