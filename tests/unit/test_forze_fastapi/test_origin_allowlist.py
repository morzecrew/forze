"""The browser-origin allowlist shared by the cookie CSRF gate and the realtime WebSocket.

A dev server picks a free port from a range, so an exact-origin list made a deployment list every
port by hand or wrap the gate. A range is allowed on a loopback host only: on a public host it
would admit every service on the machine.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_fastapi.security import CookieCsrf, OriginAllowlist
from forze_fastapi.security import value_objects

ALLOWLIST = OriginAllowlist.parse(
    [
        "https://app.example.com",
        "http://localhost:5173-5199",
        "http://127.0.0.1:*",
        "http://[::1]:3000-3001",
    ]
)


class TestMatching:
    @pytest.mark.parametrize(
        "origin",
        [
            "https://app.example.com",
            "https://APP.example.com",
            "http://localhost:5173",
            "http://localhost:5199",
            "http://localhost:5180/some/referer/path",
            "http://127.0.0.1:9",
            "http://127.0.0.1",
            "http://[::1]:3001",
        ],
    )
    def test_an_origin_on_the_list_is_allowed(self, origin: str) -> None:
        assert ALLOWLIST.allows(origin)

    @pytest.mark.parametrize(
        "origin",
        [
            "http://localhost:5172",
            "http://localhost:5200",
            "http://localhost",
            "https://localhost:5180",
            "http://evil.example:5180",
            "http://localhost.evil.example:5180",
            "http://[::1]:3002",
            "https://app.example.com.evil.example",
            "null",
            "",
        ],
    )
    def test_an_origin_off_the_list_is_refused(self, origin: str) -> None:
        assert not ALLOWLIST.allows(origin)


class TestIpv6Hosts:
    def test_an_ipv6_host_keeps_its_brackets_when_compared(self) -> None:
        # Unbracketed, `[2001:db8::5:1]` and `[2001:db8::5]:1` would both read 2001:db8::5:1.
        allowlist = OriginAllowlist.parse(["http://[2001:db8::5:1]"])

        assert allowlist.allows("http://[2001:db8::5:1]")
        assert not allowlist.allows("http://[2001:db8::5]:1")


class TestParsing:
    @pytest.mark.parametrize(
        "entry",
        [
            pytest.param("https://app.example.com:*", id="pattern-on-public-host"),
            pytest.param("https://app.example.com:8000-8100", id="range-on-public-host"),
            pytest.param("http://localhost:5199-5173", id="inverted-range"),
            pytest.param("http://localhost:0-10", id="port-zero"),
            pytest.param("http://localhost:65535-70000", id="past-the-last-port"),
            pytest.param("app.example.com", id="no-scheme"),
        ],
    )
    def test_an_entry_that_would_never_match_or_admit_too_much_is_refused(
        self, entry: str
    ) -> None:
        with pytest.raises(CoreException, match="allowed_origins"):
            OriginAllowlist.parse([entry])


class TestTheCookieGate:
    def test_a_dev_server_in_the_range_may_use_the_cookie(self) -> None:
        gate = CookieCsrf(allowed_origins={"http://localhost:5173-5199"})

        def refused(origin: str) -> str | None:
            return gate.rejection(method="POST", host="api.local:8000", origin=origin, referer=None)

        assert refused("http://localhost:5181") is None
        assert refused("http://localhost:6000") is not None

    def test_the_list_is_normalized_once_not_per_request(self, monkeypatch: Any) -> None:
        gate = CookieCsrf(allowed_origins={f"https://app{n}.example.com" for n in range(50)})
        calls: list[str] = []
        normalize = value_objects._normalize_origin  # pyright: ignore[reportPrivateUsage]

        def counting(value: str) -> str | None:
            calls.append(value)
            return normalize(value)

        monkeypatch.setattr(value_objects, "_normalize_origin", counting)
        gate.rejection(method="POST", host="api.local", origin="https://app7.example.com", referer=None)

        assert calls == ["https://app7.example.com"]
