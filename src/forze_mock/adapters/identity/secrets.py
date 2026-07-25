"""In-memory secrets port backed by :attr:`MockState.identity`."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Collection
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.secrets import (
    FULL_SECRETS_CAPABILITIES,
    DynamicSecretsPort,
    LeasedSecret,
    SecretChanged,
    SecretRef,
    SecretsAdminPort,
    SecretsCapabilities,
    SecretsChangeSource,
    SecretsPort,
    SecretValue,
    SecretVersion,
    VersionedSecretsPort,
)
from forze.base.exceptions import exc
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class MockSecretsPort(SecretsPort, VersionedSecretsPort, SecretsAdminPort):
    state: MockState

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return FULL_SECRETS_CAPABILITIES

    def _store(self) -> dict[str, str]:
        identity = self.state.identity
        secrets = identity.setdefault("secrets", {})

        if not isinstance(secrets, dict):
            raise exc.internal("Mock identity 'secrets' substore must be a dict.")

        return secrets  # pyright: ignore[reportUnknownVariableType]

    def _versions(self) -> dict[str, int]:
        identity = self.state.identity
        versions = identity.setdefault("secrets_versions", {})

        if not isinstance(versions, dict):
            raise exc.internal("Mock identity 'secrets_versions' substore must be a dict.")

        return versions  # pyright: ignore[reportUnknownVariableType]

    async def resolve_str(self, ref: SecretRef) -> str:
        with self.state.lock:
            value = self._store().get(ref.path)

        if value is None:
            raise exc.not_found(f"Secret not found: {ref.path!r}")

        return value

    async def exists(self, ref: SecretRef) -> bool:
        with self.state.lock:
            return ref.path in self._store()

    async def resolve_versioned(self, ref: SecretRef) -> SecretValue:
        with self.state.lock:
            value = self._store().get(ref.path)
            # Values seeded directly into state (no put) read as version 1.
            version = self._versions().get(ref.path, 1)

        if value is None:
            raise exc.not_found(f"Secret not found: {ref.path!r}")

        return SecretValue(text=value, version=SecretVersion(str(version)))

    async def current_version(self, ref: SecretRef) -> SecretVersion:
        return (await self.resolve_versioned(ref)).version

    async def put(self, ref: SecretRef, value: str) -> SecretVersion:
        with self.state.lock:
            self._store()[ref.path] = value
            versions = self._versions()
            version = versions.get(ref.path, 0) + 1
            versions[ref.path] = version

        return SecretVersion(str(version))


# ....................... #


@final
@attrs.define(slots=True)
class MockSecretsChangeSource(SecretsChangeSource):
    """Programmable change source: tests and DST call :meth:`emit` at exact schedule
    points; every live subscription matching the ref observes the change."""

    _subscribers: list[tuple[asyncio.Queue[SecretChanged], frozenset[str] | None]] = attrs.field(
        factory=list, init=False
    )

    def emit(self, change: SecretChanged) -> None:
        """Deliver *change* to every matching subscriber (at-least-once, unordered)."""

        for queue, paths in self._subscribers:
            if paths is None or change.ref.path in paths:
                queue.put_nowait(change)

    # ....................... #

    async def subscribe(
        self,
        refs: Collection[SecretRef] | None = None,
    ) -> AsyncIterator[SecretChanged]:
        queue: asyncio.Queue[SecretChanged] = asyncio.Queue()
        paths = None if refs is None else frozenset(ref.path for ref in refs)
        entry = (queue, paths)
        self._subscribers.append(entry)

        try:
            while True:
                yield await queue.get()

        finally:
            self._subscribers.remove(entry)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MockDynamicSecretsPort(DynamicSecretsPort):
    """Deterministic leased credentials backed by :attr:`MockState.identity`.

    Issues sequenced leases with a fixed TTL; renewal grants exactly what was asked
    while the lease is live, and a revoked or unknown lease fails closed.
    """

    state: MockState

    ttl: timedelta = timedelta(seconds=60)
    """Granted TTL at issuance."""

    @property
    def secrets_capabilities(self) -> SecretsCapabilities:
        return SecretsCapabilities(dynamic_credentials=True)

    def _leases(self) -> dict[str, dict[str, object]]:
        identity = self.state.identity
        leases = identity.setdefault("secrets_leases", {})

        if not isinstance(leases, dict):
            raise exc.internal("Mock identity 'secrets_leases' substore must be a dict.")

        return leases  # pyright: ignore[reportUnknownVariableType]

    async def issue(self, ref: SecretRef) -> LeasedSecret:
        with self.state.lock:
            leases = self._leases()
            sequence = len(leases) + 1
            lease_id = f"mock-lease/{ref.path}/{sequence}"
            leases[lease_id] = {"role": ref.path, "revoked": False}

        return LeasedSecret(
            text=f'{{"username": "{ref.path}-v{sequence}", "password": "mock-{sequence}"}}',
            lease_id=lease_id,
            ttl=self.ttl,
            renewable=True,
        )

    async def renew(self, lease_id: str, increment: timedelta) -> timedelta:
        with self.state.lock:
            lease = self._leases().get(lease_id)

            if lease is None or lease.get("revoked"):
                raise exc.not_found(f"No live lease {lease_id!r}")

        return increment

    async def revoke(self, lease_id: str) -> None:
        with self.state.lock:
            lease = self._leases().get(lease_id)

            if lease is not None:
                lease["revoked"] = True
