"""In-memory secrets port backed by :attr:`MockState.identity`."""

from __future__ import annotations

from typing import final

import attrs

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.exceptions import exc
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockSecretsPort(SecretsPort):
    state: MockState

    def _store(self) -> dict[str, str]:
        identity = self.state.identity
        secrets = identity.setdefault("secrets", {})
        assert isinstance(secrets, dict)  # nosec: B101
        return secrets  # type: ignore[return-value]

    async def resolve_str(self, ref: SecretRef) -> str:
        with self.state.lock:
            value = self._store().get(ref.path)

        if value is None:
            raise exc.not_found(f"Secret not found: {ref.path!r}")

        return value

    async def exists(self, ref: SecretRef) -> bool:
        with self.state.lock:
            return ref.path in self._store()
