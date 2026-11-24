"""The store schema pages print the render of forze's declarations.

Applications copy their migrations from ``pages/docs/reference/outbox-schema.md`` and
``store-schema.md``. Their Postgres SQL must be what ``forze schema sql`` renders from the
shapes the adapters declare, which is also what the integration tests create
(``tests/support/owned_schema.py``), so a page cannot drift from the schema the adapters are
tested against. A block fenced with a title (```` ```sql title="Your partitions" ````) holds
the application's own statements, such as partition upkeep, and is not a render.
"""

import re
import sys
from pathlib import Path
from typing import Any, Final

import pytest

from forze_postgres.adapters.counter import PostgresCounterAdapter
from forze_postgres.adapters.hlc_checkpoint import PostgresHlcCheckpointStore
from forze_postgres.adapters.idempotency import PostgresIdempotencyStore
from forze_postgres.adapters.inbox import PostgresInboxStore
from forze_postgres.adapters.outbox import PostgresOutboxStore
from forze_postgres.adapters.principal_epoch import PostgresPrincipalEpochStore
from forze_postgres.adapters.rotating_credentials import PostgresRotatingCredentialStore
from forze_postgres.adapters.store_schema import (
    counter_shape,
    hlc_checkpoint_shape,
    idempotency_shape,
    inbox_shape,
    outbox_delivery_log_shape,
    outbox_shape,
    principal_epoch_shape,
    rotating_credentials_shape,
)
from forze_postgres.execution.deps.configs import (
    PostgresCounterConfig,
    PostgresHlcCheckpointConfig,
    PostgresIdempotencyConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
    PostgresPrincipalEpochConfig,
    PostgresRotatingCredentialsConfig,
)
from forze_postgres.kernel.catalog.owned import BoundOwnedRelation, OwnedRelationShape
from forze_postgres.kernel.catalog.owned.render import install_blocks

# ----------------------- #

_PAGES: Final[Path] = Path(__file__).resolve().parents[2] / "pages" / "docs" / "reference"

_SQL_BLOCK: Final[re.Pattern[str]] = re.compile(r"```sql\n(.*?)```", re.DOTALL)

_RELATION: Final = ("app", "unused")
_EXCHANGER: Any = object()


def _words(text: str) -> str:
    return " ".join(text.split())


def _statements(text: str) -> list[str]:
    """Every statement in *text*'s untitled SQL blocks, comments dropped, whitespace folded."""

    body = re.sub(r"--[^\n]*", "", "\n".join(_SQL_BLOCK.findall(text)))

    return [_words(statement) for statement in body.split(";") if statement.strip()]


def _rendered(*tables: tuple[str, OwnedRelationShape]) -> list[str]:
    out: list[str] = []

    for name, shape in tables:
        relation = BoundOwnedRelation(route=name, schema="app", name=name, shape=shape)

        for block in install_blocks(relation):
            assert block.statement is not None
            out.append(_words(block.statement.removesuffix(";")))

    return out


def _page(name: str) -> str:
    return (_PAGES / name).read_text(encoding="utf-8")


# ....................... #


def test_the_outbox_page_prints_the_render_of_its_tables() -> None:
    postgres = _page("outbox-schema.md").split("\n## Mongo\n")[0]
    consume = PostgresOutboxConfig(relation=_RELATION, storage="consume", delivery_log=_RELATION)

    assert _statements(postgres) == _rendered(
        ("outbox", outbox_shape(PostgresOutboxConfig(relation=_RELATION))),
        ("hlc_checkpoint", hlc_checkpoint_shape(PostgresHlcCheckpointConfig(relation=_RELATION))),
        ("outbox_delivered", outbox_delivery_log_shape(consume)),
        ("inbox", inbox_shape(PostgresInboxConfig(relation=_RELATION))),
    )


def test_the_store_page_prints_the_render_of_its_tables() -> None:
    credentials = PostgresRotatingCredentialsConfig(relation=_RELATION, exchanger=_EXCHANGER)

    assert _statements(_page("store-schema.md")) == _rendered(
        ("idempotency", idempotency_shape(PostgresIdempotencyConfig(relation=_RELATION))),
        ("counters", counter_shape(PostgresCounterConfig(relation=_RELATION))),
        (
            "principal_epochs",
            principal_epoch_shape(PostgresPrincipalEpochConfig(relation=_RELATION)),
        ),
        ("rotating_credentials", rotating_credentials_shape(credentials)),
    )


@pytest.mark.parametrize(
    ("adapter", "shape"),
    [
        (PostgresOutboxStore, "outbox_shape"),
        (PostgresHlcCheckpointStore, "hlc_checkpoint_shape"),
        (PostgresInboxStore, "inbox_shape"),
        (PostgresIdempotencyStore, "idempotency_shape"),
        (PostgresCounterAdapter, "counter_shape"),
        (PostgresPrincipalEpochStore, "principal_epoch_shape"),
        (PostgresRotatingCredentialStore, "rotating_credentials_shape"),
    ],
)
def test_the_store_adapters_point_at_their_declaration_instead_of_copying_it(
    adapter: type, shape: str
) -> None:
    # The class docstring, or the module's where the class defers to it.
    doc = f"{adapter.__doc__}\n{sys.modules[adapter.__module__].__doc__}"

    assert "CREATE TABLE" not in doc
    assert f"forze_postgres.adapters.store_schema.{shape}" in doc


def test_every_titled_block_is_the_applications_own_statements() -> None:
    # A render hidden behind a title would drop out of the comparison unseen.
    titled = re.findall(r'```sql title="[^"]+"\n(.*?)```', _page("outbox-schema.md"), re.DOTALL)

    assert titled
    assert not any("CREATE INDEX" in block or "CREATE TABLE app.outbox (" in block for block in titled)
