"""Unit tests for Vault client token renewal and health (mocked hvac)."""

import asyncio
from datetime import timedelta
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

pytest.importorskip("hvac")

import forze_vault.kernel.client.client as client_module
from forze_vault.kernel.client import VaultClient, VaultConfig
from forze_vault.kernel.client.client import (
    MIN_RENEW_DELAY_SECONDS,
    RENEW_FAILURE_RETRY_SECONDS,
)

# ----------------------- #

_REAL_SLEEP = asyncio.sleep


class SleepController:
    """Deterministic replacement for ``asyncio.sleep`` in the renew loop."""

    def __init__(self) -> None:
        self.delays: list[float] = []
        self._release: asyncio.Queue[None] = asyncio.Queue()

    async def sleep(self, delay: float) -> None:
        self.delays.append(delay)
        await self._release.get()

    def release(self) -> None:
        self._release.put_nowait(None)


async def _until(cond: Callable[[], bool], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while not cond():
        if loop.time() > deadline:
            raise AssertionError("condition not met within timeout")

        await _REAL_SLEEP(0.001)


def _mock_hvac(
    *,
    renewable: bool = True,
    ttl: float = 60,
    lease_duration: Any = 40,
) -> MagicMock:
    mock = MagicMock()
    mock.auth.token.lookup_self.return_value = {
        "data": {"renewable": renewable, "ttl": ttl},
    }
    mock.auth.token.renew_self.return_value = {
        "auth": {"lease_duration": lease_duration},
    }
    return mock


def _client(**config_kwargs: Any) -> VaultClient:
    config = VaultConfig(
        url="http://127.0.0.1:8200",
        token="t",
        renew_token=True,
        **config_kwargs,
    )
    return VaultClient(config=config)

# ----------------------- #

@pytest.mark.asyncio
async def test_renews_on_half_ttl_and_reschedules_from_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctl = SleepController()
    monkeypatch.setattr(client_module.asyncio, "sleep", ctl.sleep)

    client = _client()
    mock_hvac = _mock_hvac(ttl=60, lease_duration=40)
    client._client = mock_hvac

    await client._start_token_renewal()
    assert client._renew_task is not None

    # First cadence: half the looked-up TTL.
    await _until(lambda: len(ctl.delays) == 1)
    assert ctl.delays[0] == pytest.approx(30.0)

    # After renewal, cadence comes from the renewal response lease_duration.
    ctl.release()
    await _until(lambda: len(ctl.delays) == 2)
    assert mock_hvac.auth.token.renew_self.call_count == 1
    assert ctl.delays[1] == pytest.approx(20.0)

    await client.close()
    assert client._renew_task is None

@pytest.mark.asyncio
async def test_renew_interval_overrides_ttl_derivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctl = SleepController()
    monkeypatch.setattr(client_module.asyncio, "sleep", ctl.sleep)

    client = _client(renew_interval=timedelta(seconds=5))
    client._client = _mock_hvac(ttl=60, lease_duration=40)

    await client._start_token_renewal()

    await _until(lambda: len(ctl.delays) == 1)
    assert ctl.delays[0] == pytest.approx(5.0)

    ctl.release()
    await _until(lambda: len(ctl.delays) == 2)
    assert ctl.delays[1] == pytest.approx(5.0)

    await client.close()

@pytest.mark.asyncio
async def test_renewal_failure_backs_off_and_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctl = SleepController()
    monkeypatch.setattr(client_module.asyncio, "sleep", ctl.sleep)
    mock_logger = MagicMock()
    monkeypatch.setattr(client_module, "logger", mock_logger)

    client = _client()
    mock_hvac = _mock_hvac(ttl=60)
    mock_hvac.auth.token.renew_self.side_effect = [
        RuntimeError("vault hiccup"),
        {"auth": {"lease_duration": 40}},
    ]
    client._client = mock_hvac

    await client._start_token_renewal()

    await _until(lambda: len(ctl.delays) == 1)
    ctl.release()

    # Failure: warn, back off, keep running.
    await _until(lambda: len(ctl.delays) == 2)
    assert ctl.delays[1] == pytest.approx(RENEW_FAILURE_RETRY_SECONDS)
    assert mock_logger.warning.call_count == 1
    assert client._renew_task is not None
    assert not client._renew_task.done()

    # Recovery: cadence derived from the successful renewal again.
    ctl.release()
    await _until(lambda: len(ctl.delays) == 3)
    assert ctl.delays[2] == pytest.approx(20.0)

    await client.close()

@pytest.mark.asyncio
async def test_non_renewable_token_warns_once_and_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_logger = MagicMock()
    monkeypatch.setattr(client_module, "logger", mock_logger)

    client = _client()
    client._client = _mock_hvac(renewable=False)

    await client._start_token_renewal()

    assert client._renew_task is None
    assert mock_logger.warning.call_count == 1

    await client.close()

@pytest.mark.asyncio
async def test_initialize_starts_renewal_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctl = SleepController()
    monkeypatch.setattr(client_module.asyncio, "sleep", ctl.sleep)

    mock_hvac = _mock_hvac(ttl=2, lease_duration=2)
    monkeypatch.setattr(VaultClient, "_create_client", lambda self: mock_hvac)

    client = _client()
    await client.initialize()

    assert client._renew_task is not None
    await _until(lambda: len(ctl.delays) == 1)
    assert ctl.delays[0] == pytest.approx(MIN_RENEW_DELAY_SECONDS)

    await client.close()
    assert client._renew_task is None
    assert client._client is None

@pytest.mark.asyncio
async def test_initialize_without_renew_token_starts_no_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_hvac = _mock_hvac()
    monkeypatch.setattr(VaultClient, "_create_client", lambda self: mock_hvac)

    config = VaultConfig(url="http://127.0.0.1:8200", token="t")
    client = VaultClient(config=config)
    await client.initialize()

    assert client._renew_task is None
    mock_hvac.auth.token.lookup_self.assert_not_called()

    await client.close()

@pytest.mark.asyncio
async def test_close_cancels_pending_renewal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctl = SleepController()
    monkeypatch.setattr(client_module.asyncio, "sleep", ctl.sleep)

    client = _client()
    client._client = _mock_hvac()

    await client._start_token_renewal()
    task = client._renew_task
    assert task is not None

    await _until(lambda: len(ctl.delays) == 1)
    await client.close()

    assert task.cancelled()
    assert client._renew_task is None
    assert client._client is None

# ----------------------- #
# health()

@pytest.mark.asyncio
async def test_health_ok() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.sys.read_health_status.return_value = {
        "initialized": True,
        "sealed": False,
    }
    client._client = mock_hvac

    assert await client.health() == ("ok", True)

@pytest.mark.asyncio
async def test_health_sealed() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.sys.read_health_status.return_value = {
        "initialized": True,
        "sealed": True,
    }
    client._client = mock_hvac

    assert await client.health() == ("sealed", False)

@pytest.mark.asyncio
async def test_health_not_initialized() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.sys.read_health_status.return_value = {
        "initialized": False,
        "sealed": False,
    }
    client._client = mock_hvac

    assert await client.health() == ("not initialized", False)

@pytest.mark.asyncio
async def test_health_never_raises() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))
    mock_hvac = MagicMock()
    mock_hvac.sys.read_health_status.side_effect = RuntimeError("conn refused")
    client._client = mock_hvac

    msg, ok = await client.health()
    assert ok is False
    assert "conn refused" in msg

@pytest.mark.asyncio
async def test_health_uninitialized_client() -> None:
    client = VaultClient(config=VaultConfig(url="http://127.0.0.1:8200", token="t"))

    msg, ok = await client.health()
    assert ok is False
    assert "not initialized" in msg

# ----------------------- #
# config validation

def test_renew_interval_must_be_positive() -> None:
    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException):
        VaultConfig(
            url="http://127.0.0.1:8200",
            token="t",
            renew_token=True,
            renew_interval=timedelta(seconds=0),
        )
