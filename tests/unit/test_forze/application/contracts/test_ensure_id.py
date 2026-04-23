"""Tests for ensure / ensure_many id validation helpers."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    assert_unique_ensure_ids,
    require_create_id_for_ensure,
)
from forze.base.errors import ValidationError
from forze.domain.models import CreateDocumentCmd


def test_require_create_id_for_ensure_accepts_id() -> None:
    u = uuid4()
    d = CreateDocumentCmd(id=u)
    assert require_create_id_for_ensure(d) == u


def test_require_create_id_for_ensure_rejects_missing() -> None:
    with pytest.raises(ValidationError, match="id"):
        require_create_id_for_ensure(CreateDocumentCmd())


def test_assert_unique_ensure_ids_rejects_dupes() -> None:
    u = uuid4()
    with pytest.raises(ValidationError, match="distinct"):
        assert_unique_ensure_ids([CreateDocumentCmd(id=u), CreateDocumentCmd(id=u)])


def test_assert_unique_ensure_ids_accepts_distinct() -> None:
    assert_unique_ensure_ids(
        [CreateDocumentCmd(id=uuid4()), CreateDocumentCmd(id=uuid4())]
    )
