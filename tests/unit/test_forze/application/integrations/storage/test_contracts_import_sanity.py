"""Ensure core storage contracts do not import optional integration dependencies."""

import importlib
import sys


def test_storage_contracts_import_without_magic(monkeypatch) -> None:
    if "magic" in sys.modules:
        monkeypatch.delitem(sys.modules, "magic")

    module = importlib.import_module("forze.application.contracts.storage")

    assert "magic" not in sys.modules
    assert module.StoragePort is not None
