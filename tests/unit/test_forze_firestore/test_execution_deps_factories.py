"""Unit tests for Firestore document dependency factories."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.adapters.document import FirestoreDocumentAdapter
from forze_firestore.execution.deps import (
    ConfigurableFirestoreDocument,
    ConfigurableFirestoreReadOnlyDocument,
    firestore_txmanager,
)
from forze_firestore.execution.deps.configs import (
    FirestoreDocumentConfig,
    FirestoreReadOnlyDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Read(ReadDocument):
    name: str


class _Domain(Document):
    name: str


class _Create(CreateDocumentCmd):
    name: str


class _Update(BaseDTO):
    name: str | None = None


def _rw_spec(*, history_enabled: bool = False) -> DocumentSpec:
    return DocumentSpec(
        name="docs",
        read=_Read,
        write={
            "domain": _Domain,
            "create_cmd": _Create,
            "update_cmd": _Update,
        },
        history_enabled=history_enabled,
    )


def _ctx(client: object = object()) -> ExecutionContext:
    return context_from_deps(__import__(
            "forze.application.execution",
            fromlist=["Deps"],).Deps.plain({FirestoreClientDepKey: client}),
    )


def test_rejects_mapping_config() -> None:
    with pytest.raises(TypeError, match="FirestoreReadOnlyDocumentConfig"):
        ConfigurableFirestoreReadOnlyDocument(config={"read": ("(default)", "c")})


class TestConfigurableFirestoreReadOnlyDocument:
    def test_builds_read_only_adapter(self) -> None:
        factory = ConfigurableFirestoreReadOnlyDocument(
            config=FirestoreReadOnlyDocumentConfig(read=("(default)", "coll")),
        )
        adapter = factory(_ctx(), _rw_spec())
        assert isinstance(adapter, FirestoreDocumentAdapter)
        assert adapter.write_gw is None


class TestConfigurableFirestoreDocument:
    def test_requires_write_spec(self) -> None:
        factory = ConfigurableFirestoreDocument(
            config=FirestoreDocumentConfig(
                read=("(default)", "r"),
                write=("(default)", "w"),
            ),
        )
        spec = DocumentSpec(name="ro", read=_Read)

        with pytest.raises(CoreException, match="Write relation"):
            factory(_ctx(), spec)

    def test_warns_when_history_enabled_without_relation(self) -> None:
        factory = ConfigurableFirestoreDocument(
            config=FirestoreDocumentConfig(
                read=("(default)", "r"),
                write=("(default)", "w"),
            ),
        )
        mock_logger = MagicMock()

        with patch(
            "forze_firestore.execution.deps.factories.document.logger",
            mock_logger,
        ):
            factory(_ctx(), _rw_spec(history_enabled=True))

        joined = " ".join(str(c) for c in mock_logger.warning.call_args_list)
        assert "History relation not found" in joined

    def test_warns_when_history_relation_but_disabled(self) -> None:
        factory = ConfigurableFirestoreDocument(
            config=FirestoreDocumentConfig(
                read=("(default)", "r"),
                write=("(default)", "w"),
                history=("(default)", "h"),
            ),
        )
        mock_logger = MagicMock()

        with patch(
            "forze_firestore.execution.deps.factories.document.logger",
            mock_logger,
        ):
            factory(_ctx(), _rw_spec(history_enabled=False))

        joined = " ".join(str(c) for c in mock_logger.warning.call_args_list)
        assert "history is disabled" in joined


class TestFirestoreTxManager:
    def test_resolves_tx_manager(self) -> None:
        client = MagicMock()
        port = firestore_txmanager(_ctx(client))
        assert port.client is client
