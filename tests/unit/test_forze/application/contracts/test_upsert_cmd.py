"""Tests for upsert validation helpers."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    assert_unique_upsert_pairs,
    require_create_id_for_upsert,
)
from forze.base.errors import ValidationError
from forze.domain.models import CreateDocumentCmd


def test_require_create_id_for_upsert_ok() -> None:
    u = uuid4()
    assert require_create_id_for_upsert(CreateDocumentCmd(id=u)) == u


def test_require_create_id_for_upsert_rejects_missing() -> None:
    with pytest.raises(ValidationError, match="id"):
        require_create_id_for_upsert(CreateDocumentCmd())


def test_assert_unique_upsert_pairs_rejects_dupes() -> None:
    u = uuid4()
    with pytest.raises(ValidationError, match="distinct"):
        assert_unique_upsert_pairs(
            [(CreateDocumentCmd(id=u), object()), (CreateDocumentCmd(id=u), object())]
        )


def test_assert_unique_upsert_pairs_accepts_distinct() -> None:
    assert_unique_upsert_pairs(
        [
            (CreateDocumentCmd(id=uuid4()), object()),
            (CreateDocumentCmd(id=uuid4()), object()),
        ]
    )
