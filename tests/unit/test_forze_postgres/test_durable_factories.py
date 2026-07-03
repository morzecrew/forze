"""Postgres durable dep factories: client/keyring resolution + fail-closed encryption."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.application.contracts.crypto import KeyringDepKey
from forze.base.exceptions import CoreException
from forze_postgres.adapters.durable import (
    PostgresDurableFunctionStepAdapter,
    PostgresDurableRunStore,
    PostgresDurableScheduleStore,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.execution.deps.factories.durable import (
    ConfigurablePostgresDurableRun,
    ConfigurablePostgresDurableSchedule,
    ConfigurablePostgresDurableStep,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey

# ----------------------- #


def _ctx(*, keyring: bool) -> Any:
    """A fake execution context that provides a client and (optionally) a keyring."""
    ctx = MagicMock()
    client, keyring_obj = MagicMock(name="client"), MagicMock(name="keyring")

    def _provide(key: Any) -> Any:
        if key is PostgresClientDepKey:
            return client
        if key is KeyringDepKey:
            return keyring_obj
        raise KeyError(key)

    ctx.deps.provide.side_effect = _provide
    ctx.deps.exists.side_effect = lambda key: keyring and key is KeyringDepKey
    ctx.inv_ctx.get_tenant = lambda: None
    return ctx


# ....................... #


class TestDurableStepFactory:
    def test_plaintext_builds_without_a_cipher(self) -> None:
        factory = ConfigurablePostgresDurableStep(
            config=PostgresDurableStepConfig(relation=("public", "durable_step"))
        )

        adapter = factory(_ctx(keyring=False))

        assert isinstance(adapter, PostgresDurableFunctionStepAdapter)
        assert adapter.cipher is None

    def test_encrypt_with_keyring_seals(self) -> None:
        factory = ConfigurablePostgresDurableStep(
            config=PostgresDurableStepConfig(
                relation=("public", "durable_step"), encrypt=True
            )
        )

        adapter = factory(_ctx(keyring=True))

        assert adapter.cipher is not None

    def test_encrypt_without_keyring_fails_closed(self) -> None:
        factory = ConfigurablePostgresDurableStep(
            config=PostgresDurableStepConfig(
                relation=("public", "durable_step"), encrypt=True
            )
        )

        with pytest.raises(CoreException, match="keyring"):
            factory(_ctx(keyring=False))


class TestDurableRunFactory:
    def test_plaintext_builds_without_a_cipher(self) -> None:
        factory = ConfigurablePostgresDurableRun(
            config=PostgresDurableRunConfig(relation=("public", "durable_run"))
        )

        store = factory(_ctx(keyring=False))

        assert isinstance(store, PostgresDurableRunStore)
        assert store.cipher is None

    def test_encrypt_with_keyring_seals(self) -> None:
        factory = ConfigurablePostgresDurableRun(
            config=PostgresDurableRunConfig(
                relation=("public", "durable_run"), encrypt=True
            )
        )

        store = factory(_ctx(keyring=True))

        assert store.cipher is not None

    def test_encrypt_without_keyring_fails_closed(self) -> None:
        factory = ConfigurablePostgresDurableRun(
            config=PostgresDurableRunConfig(
                relation=("public", "durable_run"), encrypt=True
            )
        )

        with pytest.raises(CoreException, match="keyring"):
            factory(_ctx(keyring=False))


class TestDurableScheduleFactory:
    def test_builds_the_schedule_store(self) -> None:
        factory = ConfigurablePostgresDurableSchedule(
            config=PostgresDurableScheduleConfig(
                relation=("public", "durable_schedule")
            )
        )

        store = factory(_ctx(keyring=False))

        assert isinstance(store, PostgresDurableScheduleStore)
