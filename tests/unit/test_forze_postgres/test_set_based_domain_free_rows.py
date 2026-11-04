"""The Postgres write gateway dumps set-based insert rows from the create payloads only when its
codecs neither encrypt nor materialize a field; any other codec builds the domain models."""

from datetime import UTC, datetime
from uuid import uuid4

import attrs

from forze.base.serialization import PydanticModelCodec
from tests.unit.test_forze_postgres.test_kernel_gateways_write import (
    MyCreateDoc,
    MyDoc,
    _build_gateway,
)

_AT = datetime(2026, 1, 1, tzinfo=UTC)


class _SealingCodec(PydanticModelCodec[MyDoc]):
    """Stands for any codec that does more than pydantic's dump, as an encrypting one does."""


def _rows(gw: object) -> object:
    build = gw._PostgresWriteGateway__rows_without_domains  # type: ignore[attr-defined]

    return build([MyCreateDoc(name="a")], [uuid4()], now=_AT)


def test_plain_codecs_dump_the_rows_from_the_payloads() -> None:
    gw, _ = _build_gateway()

    rows = _rows(gw)

    assert isinstance(rows, list) and rows[0]["name"] == "a" and rows[0]["created_at"] == _AT


def test_a_codec_that_does_more_than_dump_leaves_the_rows_to_the_domain() -> None:
    gw, _ = _build_gateway()

    assert _rows(attrs.evolve(gw, codec=_SealingCodec(MyDoc))) is None
    assert _rows(attrs.evolve(gw, create_codec=_SealingCodec(MyDoc))) is None
