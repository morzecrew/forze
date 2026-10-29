"""Tests for :class:`~forze_firestore.execution.deps.module.FirestoreDepsModule`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.transaction import (
    TX_LOCK_UNSUPPORTED_CODE,
    TransactionManagerDepKey,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_firestore.execution.deps.configs import (
    FirestoreDocumentConfig,
    FirestoreReadOnlyDocumentConfig,
)
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.module import FirestoreDepsModule
from tests.support.execution_context import frozen_deps_from_deps


def test_firestore_deps_module_registers_client_and_documents() -> None:
    client = MagicMock()
    module = FirestoreDepsModule(
        client=client,
        ro_documents={
            "readonly": FirestoreReadOnlyDocumentConfig(
                read=("(default)", "ro_coll"),
            ),
        },
        rw_documents={
            "writable": FirestoreDocumentConfig(
                read=("(default)", "rw_coll"),
                write=("(default)", "rw_coll"),
                history=("(default)", "rw_hist"),
            ),
        },
        tx={"writable"},
    )
    registration = module()
    resolved = frozen_deps_from_deps(registration)
    assert resolved.provide(FirestoreClientDepKey) is client
    deps = registration
    assert DocumentQueryDepKey in deps.routed_deps
    assert DocumentCommandDepKey in deps.routed_deps
    assert TransactionManagerDepKey in deps.routed_deps
    assert "readonly" in deps.routed_deps[DocumentQueryDepKey]
    assert "writable" in deps.routed_deps[DocumentCommandDepKey]

    # Firestore's manager has no transaction-scoped lock, so one asked of its route is refused.
    with pytest.raises(CoreException) as caught:
        ExecutionContext(deps=resolved).tx_locks("writable")

    assert caught.value.code == TX_LOCK_UNSUPPORTED_CODE
