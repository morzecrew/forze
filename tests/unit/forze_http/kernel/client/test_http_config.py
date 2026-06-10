"""Tests for :class:`~forze_http.kernel.client.value_objects.HttpxConfig` defaults."""

import httpx
import pytest

from forze_http.kernel.client import HttpxClient, HttpxConfig

# ----------------------- #


def test_follow_redirects_defaults_to_false() -> None:
    # Security default: httpx only strips Authorization cross-origin, so custom
    # credential headers (e.g. X-API-Key) would follow a malicious 30x to
    # another host. Redirect following is explicit opt-in.
    assert HttpxConfig().follow_redirects is False


def test_follow_redirects_explicit_true_is_honored() -> None:
    assert HttpxConfig(follow_redirects=True).follow_redirects is True


@pytest.mark.asyncio
async def test_client_defaults_to_no_redirect_following() -> None:
    client = HttpxClient()
    await client.initialize("http://api.local")

    inner: httpx.AsyncClient = client._HttpxClient__client  # type: ignore[attr-defined]

    try:
        assert inner.follow_redirects is False

    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_client_honors_explicit_redirect_opt_in() -> None:
    client = HttpxClient()
    await client.initialize(
        "http://api.local",
        config=HttpxConfig(follow_redirects=True),
    )

    inner: httpx.AsyncClient = client._HttpxClient__client  # type: ignore[attr-defined]

    try:
        assert inner.follow_redirects is True

    finally:
        await client.aclose()
