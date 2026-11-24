"""The durable schema page prints the render of forze's declarations.

Applications copy their migrations from ``pages/docs/reference/durable-schema.md``. Its Postgres
SQL must be what ``forze schema sql`` renders from the shapes the adapters declare, which is also
what the integration tests create (``tests/support/durable_schema.py``), so the page cannot
drift from the schema the adapters are tested against. The Mongo half has no declaration yet,
so it is compared with the Mongo adapters' docstrings.
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
from forze_postgres.kernel.catalog.owned import BoundOwnedRelation
from forze_postgres.kernel.catalog.owned.render import install_blocks
from tests.support.durable_schema import DurableTable, durable_shape

# ----------------------- #

_PAGE: Final[Path] = (
    Path(__file__).resolve().parents[2] / "pages" / "docs" / "reference" / "durable-schema.md"
)

_PAGE_TEXT: Final[str] = _PAGE.read_text(encoding="utf-8")
_POSTGRES: Final[str] = _PAGE_TEXT.split("\n## Mongo\n")[0]

_SQL_BLOCK: Final[re.Pattern[str]] = re.compile(r"```sql\n(.*?)```", re.DOTALL)
_MONGO_INDEX: Final[re.Pattern[str]] = re.compile(r"db\.(\S+?)\.createIndex\(([^()]*)\)")


def _words(text: str) -> str:
    return " ".join(text.split())


def _statements(text: str) -> list[str]:
    """Every statement in *text*'s SQL blocks, comments dropped and whitespace folded."""

    body = re.sub(r"--[^\n]*", "", "\n".join(_SQL_BLOCK.findall(text)))

    return [_words(statement) for statement in body.split(";") if statement.strip()]


def _rendered() -> list[str]:
    out: list[str] = []
    tables: tuple[tuple[DurableTable, str], ...] = (
        ("run", "durable_run"),
        ("step", "durable_step"),
        ("schedule", "durable_schedule"),
    )

    for kind, name in tables:
        relation = BoundOwnedRelation(
            route=name, schema="public", name=name, shape=durable_shape(kind)
        )

        for block in install_blocks(relation):
            assert block.statement is not None
            out.append(_words(block.statement.removesuffix(";")))

    return out


def _mongo_indexes(text: str) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}

    for name, arguments in _MONGO_INDEX.findall(text):
        found.setdefault(name, []).append(re.sub(r"\s+|,(?=\s*$)", "", arguments))

    return found


def _doc(adapter: type) -> str:
    assert adapter.__doc__ is not None
    return adapter.__doc__


# ....................... #


def test_the_page_prints_the_render_of_the_postgres_declarations() -> None:
    assert _statements(_POSTGRES) == _rendered()


@pytest.mark.parametrize(
    "adapter",
    [PostgresDurableRunStore, PostgresDurableFunctionStepAdapter, PostgresDurableScheduleStore],
)
def test_the_postgres_adapters_point_at_their_declaration_instead_of_copying_it(
    adapter: type,
) -> None:
    doc = _doc(adapter)

    assert "CREATE" not in doc
    assert "forze_postgres.adapters.durable.schema." in doc


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


def test_the_page_documents_every_mongo_collection_it_names() -> None:
    assert set(_mongo_indexes(_PAGE_TEXT)) == {"durable_run", "durable_step", "durable_schedule"}


@pytest.mark.parametrize(
    "adapter", [MongoDurableRunStore, MongoDurableFunctionStepAdapter, MongoDurableScheduleStore]
)
def test_every_mongo_index_in_a_docstring_is_one_the_comparison_reads(adapter: type) -> None:
    # An index the pattern cannot read would drop out of both sides and pass unseen.
    text = _doc(adapter)

    assert text.count("createIndex") == len(_MONGO_INDEX.findall(text))


def test_every_mongo_index_on_the_page_is_one_the_comparison_reads() -> None:
    assert _PAGE_TEXT.count("createIndex") == len(_MONGO_INDEX.findall(_PAGE_TEXT))
