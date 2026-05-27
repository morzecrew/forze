"""Tests for :class:`~forze_firestore.execution.deps.module.FirestoreDepsModule`."""

from __future__ import annotations

from unittest.mock import MagicMock

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.module import FirestoreDepsModule


def test_firestore_deps_module_registers_client_and_documents() -> None:
    client = MagicMock()
    module = FirestoreDepsModule(
        client=client,
        ro_documents={
            "readonly": {"read": ("(default)", "ro_coll")},
        },
        rw_documents={
            "writable": {
                "read": ("(default)", "rw_coll"),
                "write": ("(default)", "rw_coll"),
                "history": ("(default)", "rw_hist"),
            },
        },
        tx={"writable"},
    )
    deps = module()
    assert deps.provide(FirestoreClientDepKey) is client
    assert DocumentQueryDepKey in deps.routed_deps
    assert DocumentCommandDepKey in deps.routed_deps
    assert TransactionManagerDepKey in deps.routed_deps
    assert "readonly" in deps.routed_deps[DocumentQueryDepKey]
    assert "writable" in deps.routed_deps[DocumentCommandDepKey]
