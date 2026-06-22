"""Unit tests for :mod:`forze.application.contracts.transport.frames`."""

import pytest

from forze.application.contracts.transport import FrameErr, FrameOk, guard_frame
from forze.base.exceptions import CoreException, exc

# ----------------------- #


async def test_success_returns_frame_ok() -> None:
    calls: list[tuple[CoreException | None, BaseException]] = []

    async def run() -> str:
        return "ack"

    outcome = await guard_frame(run, on_server_error=lambda c, e: calls.append((c, e)))

    assert isinstance(outcome, FrameOk)
    assert outcome.value == "ack"
    assert calls == []  # no logging on the happy path


# ....................... #


async def test_client_safe_core_exception_is_not_logged() -> None:
    calls: list[tuple[CoreException | None, BaseException]] = []

    async def run() -> str:
        raise exc.validation("bad payload", details={"field": "x"})

    outcome = await guard_frame(run, on_server_error=lambda c, e: calls.append((c, e)))

    assert isinstance(outcome, FrameErr)
    assert outcome.envelope.server_error is False
    assert outcome.envelope.detail == "bad payload"
    assert calls == []  # client-safe errors never hit the server-error hook


# ....................... #


async def test_server_core_exception_invokes_hook() -> None:
    calls: list[tuple[CoreException | None, BaseException]] = []

    error = exc.internal("kaboom")

    async def run() -> str:
        raise error

    outcome = await guard_frame(run, on_server_error=lambda c, e: calls.append((c, e)))

    assert isinstance(outcome, FrameErr)
    assert outcome.envelope.server_error is True
    assert calls == [(error, error)]  # (core, exc) for a server-side CoreException


# ....................... #


async def test_unhandled_exception_is_generic_and_logged() -> None:
    calls: list[tuple[CoreException | None, BaseException]] = []

    boom = RuntimeError("unexpected")

    async def run() -> str:
        raise boom

    outcome = await guard_frame(run, on_server_error=lambda c, e: calls.append((c, e)))

    assert isinstance(outcome, FrameErr)
    assert outcome.envelope.server_error is True
    assert outcome.envelope.code == "core.internal"
    assert calls == [(None, boom)]  # core is None for an unhandled error


# ....................... #


async def test_hook_is_optional() -> None:
    async def core_boom() -> str:
        raise exc.internal("kaboom")

    async def raw_boom() -> str:
        raise RuntimeError("unexpected")

    # neither path should raise when no hook is provided
    assert isinstance(await guard_frame(core_boom), FrameErr)
    assert isinstance(await guard_frame(raw_boom), FrameErr)
