"""Unit tests for the ``native`` escape hatch on the Temporal clients.

The property exists to delete one bug class: an application that needs SDK surface the
port omits, building a *second* ``Client.connect`` by hand and silently losing the
configured data converter (payload encryption), the interceptor stack, and the rpc
metadata. So the assertion that matters here is **identity** — ``native`` is the very
object the port methods drive, not an equivalent one.
"""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from forze.base.exceptions import CoreException, ExceptionKind

pytest.importorskip("temporalio")

from forze.application.contracts.secrets import SecretRef
from forze_temporal.kernel.client import RoutedTemporalClient, TemporalClient

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/temporal")


# ....................... #


class _MemSecrets:
    """Minimal :class:`SecretsPort` slice: tenant id -> Temporal host."""

    def __init__(self, hosts: dict[UUID, str]) -> None:
        self.hosts = hosts

    async def resolve_str(self, ref: SecretRef) -> str:
        for tid, host in self.hosts.items():
            if ref.path == f"tenants/{tid}/temporal":
                return host

        raise RuntimeError("missing")

    async def exists(self, ref: SecretRef) -> bool:
        return any(ref.path == f"tenants/{tid}/temporal" for tid in self.hosts)


# ....................... #


def _connect_patch(backend: MagicMock):
    return patch(
        "forze_temporal.kernel.client.client.Client.connect",
        new_callable=AsyncMock,
        return_value=backend,
    )


# ----------------------- #


class TestTemporalClientNative:
    """:attr:`TemporalClient.native`."""

    @pytest.mark.asyncio
    async def test_native_is_the_connected_client(self) -> None:
        """``native`` hands back exactly what ``Client.connect`` returned."""

        backend = MagicMock()

        with _connect_patch(backend):
            client = TemporalClient()
            await client.initialize("localhost:7233")

        assert client.native is backend

    @pytest.mark.asyncio
    async def test_native_is_the_same_client_the_port_drives(self) -> None:
        """The escape hatch and the port surface share one connection.

        This is the footgun test in unit form: a ``native`` that resolved to a *second*
        client would carry a different codec and interceptor stack, so the call recorded
        on the backend below would land on an object the port never touches.
        """

        backend = MagicMock()

        with _connect_patch(backend):
            client = TemporalClient()
            await client.initialize("localhost:7233")

        client.get_workflow_handle("wf-1")

        assert client.native is backend
        assert backend.get_workflow_handle.call_count == 1

    def test_native_before_initialize_raises_not_initialized(self) -> None:
        """Reaching for the hatch before startup raises the standard error."""

        client = TemporalClient()

        with pytest.raises(CoreException, match="not initialized") as excinfo:
            _ = client.native

        assert excinfo.value.kind is ExceptionKind.INTERNAL

    @pytest.mark.asyncio
    async def test_native_after_close_raises_not_initialized(self) -> None:
        """``close`` drops the reference, so the hatch closes with the client."""

        with _connect_patch(MagicMock()):
            client = TemporalClient()
            await client.initialize("localhost:7233")

        await client.close()

        with pytest.raises(CoreException, match="not initialized"):
            _ = client.native


# ....................... #


class TestRoutedTemporalClientNative:
    """:attr:`RoutedTemporalClient.native` — the same hatch, per route."""

    @staticmethod
    def _routed(
        hosts: dict[UUID, str],
        tenant: "list[UUID | None]",
    ) -> RoutedTemporalClient:
        return RoutedTemporalClient(
            secrets=_MemSecrets(hosts),  # type: ignore[arg-type]
            secret_ref_for_tenant=_ref,
            tenant_provider=lambda: tenant[0],
            max_cached_tenants=4,
        )

    @pytest.mark.asyncio
    async def test_native_returns_the_active_tenants_client(self) -> None:
        """Each route's hatch is that route's own connection, never another tenant's."""

        tenant: list[UUID | None] = [None]
        routed = self._routed({_T1: "host-a:7233", _T2: "host-b:7233"}, tenant)
        await routed.startup()

        backends: dict[str, MagicMock] = {}

        async def _fake_connect(host: str, **_: object) -> MagicMock:
            backend = MagicMock(name=host)
            backends[host] = backend

            return backend

        try:
            with patch(
                "forze_temporal.kernel.client.client.Client.connect",
                new=_fake_connect,
            ):
                tenant[0] = _T1
                await routed.health()
                tenant[0] = _T2
                await routed.health()

                tenant[0] = _T1
                assert routed.native is backends["host-a:7233"]

                tenant[0] = _T2
                assert routed.native is backends["host-b:7233"]

        finally:
            await routed.close()

    @pytest.mark.asyncio
    async def test_native_requires_startup(self) -> None:
        """Same guard as every other routed call."""

        tenant: list[UUID | None] = [_T1]
        routed = self._routed({_T1: "host-a:7233"}, tenant)

        with pytest.raises(CoreException, match="not started"):
            _ = routed.native

    @pytest.mark.asyncio
    async def test_native_without_a_tenant_raises(self) -> None:
        """No tenant bound is a caller error, not a silent fallback to some route."""

        tenant: list[UUID | None] = [None]
        routed = self._routed({_T1: "host-a:7233"}, tenant)
        await routed.startup()

        try:
            with pytest.raises(CoreException, match="Tenant ID is required"):
                _ = routed.native

        finally:
            await routed.close()

    @pytest.mark.asyncio
    async def test_native_before_the_route_connected_raises(self) -> None:
        """Sync property, so it cannot open a connection — it says so instead."""

        tenant: list[UUID | None] = [_T1]
        routed = self._routed({_T1: "host-a:7233"}, tenant)
        await routed.startup()

        try:
            with pytest.raises(CoreException, match="No Temporal client for this tenant"):
                _ = routed.native

        finally:
            await routed.close()
