"""Mongo rotation target: dual-user composition, the bounded updateUser, config gates."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("pymongo")

from forze.application.contracts.secrets import (
    PendingCredential,
    SecretRef,
    SecretVersion,
)
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import MappingSecrets
from forze_mongo.adapters.rotation_target import MongoRotationTarget
from forze_mongo.kernel.uri import (
    mongo_uri_password,
    mongo_uri_username,
    with_mongo_credentials,
)

# ----------------------- #

_PENDING = PendingCredential(ref=SecretRef("db/uri.pending"), version=SecretVersion("v2"))
_URI = "mongodb://app_a:old@db:27017/app?replicaSet=rs0&authSource=admin"


class _FakeDatabase:
    def __init__(self) -> None:
        self.commands: list[Mapping[str, Any]] = []

    async def command(self, command: Mapping[str, Any], *args: Any, **kwargs: Any) -> Any:
        _ = args, kwargs
        self.commands.append(command)

        return {"ok": 1}


class _FakeClient:
    def __init__(self) -> None:
        self.database = _FakeDatabase()
        self.requested: list[str | None] = []

    async def db(self, name: str | None = None) -> _FakeDatabase:
        self.requested.append(name)

        return self.database


def _target(
    secrets: MappingSecrets | None = None,
    **overrides: Any,
) -> tuple[MongoRotationTarget, _FakeClient]:
    client = _FakeClient()
    options: dict[str, Any] = {"user_pair": ("app_a", "app_b")}
    options.update(overrides)
    target = MongoRotationTarget(
        secrets=secrets or MappingSecrets(data={}),
        client=client,  # type: ignore[arg-type]
        **options,
    )

    return target, client


class TestUriHelper:
    def test_swapping_credentials_preserves_every_other_byte(self) -> None:
        """A rotation changes who you are, never where you connect."""

        swapped = with_mongo_credentials(_URI, username="app_b", password="fresh")

        assert swapped == "mongodb://app_b:fresh@db:27017/app?replicaSet=rs0&authSource=admin"

    def test_credentials_are_percent_encoded(self) -> None:
        """Minted passwords are raw entropy and routinely contain ``/``, ``:`` and ``@`` —
        left unescaped they would silently repoint the URI."""

        swapped = with_mongo_credentials(_URI, username="a/b", password="p@s:s/w")

        assert "a%2Fb:p%40s%3As%2Fw@db:27017" in swapped
        assert mongo_uri_username(swapped) == "a/b"
        assert mongo_uri_password(swapped) == "p@s:s/w"

    def test_srv_uris_survive(self) -> None:
        """``mongodb+srv`` cannot be rebuilt from a parsed form, so the swap is textual."""

        srv = "mongodb+srv://u:p@cluster.example.net/app?retryWrites=true"

        assert with_mongo_credentials(srv, username="v", password="q") == (
            "mongodb+srv://v:q@cluster.example.net/app?retryWrites=true"
        )

    def test_a_uri_without_credentials_is_still_addressable(self) -> None:
        bare = "mongodb://db:27017/app"

        assert with_mongo_credentials(bare, username="u", password="p") == (
            "mongodb://u:p@db:27017/app"
        )

    def test_an_at_sign_outside_the_authority_is_not_userinfo(self) -> None:
        """A '@' in the path or options must not be mistaken for a credential separator."""

        odd = "mongodb://db:27017/app?appName=team@example"

        assert with_mongo_credentials(odd, username="u", password="p") == (
            "mongodb://u:p@db:27017/app?appName=team@example"
        )

    def test_a_non_mongo_secret_fails_closed(self) -> None:
        with pytest.raises(CoreException, match="not a MongoDB connection string"):
            mongo_uri_username("postgresql://app:pw@db/app")


class TestCompose:
    async def test_dual_user_flips_to_the_idle_user(self) -> None:
        target, _ = _target()

        pending = await target.compose(None, current=_URI, minted="fresh")

        assert mongo_uri_username(pending) == "app_b"
        assert mongo_uri_password(pending) == "fresh"
        assert "replicaSet=rs0" in pending

        flipped = await target.compose(None, current=pending, minted="fresher")
        assert mongo_uri_username(flipped) == "app_a"

    async def test_single_user_keeps_the_user(self) -> None:
        target, _ = _target(user_pair=None, single_user_degraded=True)

        pending = await target.compose(None, current="mongodb://app:old@db/app", minted="fresh")

        assert mongo_uri_username(pending) == "app"

    async def test_unknown_user_fails_closed(self) -> None:
        target, _ = _target()

        with pytest.raises(CoreException, match="user pair"):
            await target.compose(None, current="mongodb://other:x@db/app", minted="fresh")


class TestApply:
    async def test_sends_a_bounded_update_user(self) -> None:
        secrets = MappingSecrets(data={"db/uri.pending": "mongodb://app_b:fresh-pw@db/app"})
        target, client = _target(secrets, apply_max_time=timedelta(seconds=7))

        await target.apply(None, _PENDING)

        assert client.requested == ["admin"]
        command = client.database.commands[0]

        assert command["updateUser"] == "app_b"
        assert command["pwd"] == "fresh-pw"
        # The bound is server-side: a command the server has not finished within it is
        # killed and never lands, which is what the reconfirmation window relies on.
        assert command["maxTimeMS"] == 7000

    async def test_the_users_database_is_configurable(self) -> None:
        secrets = MappingSecrets(data={"db/uri.pending": "mongodb://app_b:pw@db/app"})
        target, client = _target(secrets, user_database="ops")

        await target.apply(None, _PENDING)

        assert client.requested == ["ops"]

    async def test_a_pending_uri_without_a_password_fails_closed(self) -> None:
        secrets = MappingSecrets(data={"db/uri.pending": "mongodb://app_b@db/app"})
        target, client = _target(secrets)

        with pytest.raises(CoreException, match="no password"):
            await target.apply(None, _PENDING)

        assert client.database.commands == []


class TestConfig:
    def test_requires_a_mode(self) -> None:
        with pytest.raises(CoreException, match="single_user_degraded"):
            MongoRotationTarget(
                secrets=MappingSecrets(data={}),
                client=_FakeClient(),  # type: ignore[arg-type]
            )

    def test_rejects_both_modes(self) -> None:
        with pytest.raises(CoreException, match="not both"):
            _target(single_user_degraded=True)

    def test_rejects_identical_users(self) -> None:
        with pytest.raises(CoreException, match="distinct"):
            _target(user_pair=("app", "app"))

    def test_the_verify_timeout_must_be_positive(self) -> None:
        """Verify-before-promote is the gate; an unbounded or zero verify is not a gate."""

        for bad in (timedelta(0), timedelta(seconds=-1)):
            with pytest.raises(CoreException, match="Verify timeout must be positive"):
                _target(verify_timeout=bad)

    def test_the_dispatch_allowance_must_not_be_negative(self) -> None:
        """It is added to the server-side bound to declare apply_latency_bound, so a
        negative value would understate the window the rotator reconfirms over."""

        with pytest.raises(CoreException, match="must not be negative"):
            _target(dispatch_allowance=timedelta(seconds=-1))

        # Zero is legal: it means "no client-side wait is budgeted beyond maxTimeMS".
        target, _ = _target(dispatch_allowance=timedelta(0))
        assert target.apply_latency_bound == target.apply_max_time

    def test_the_apply_bound_has_no_unbounded_escape_hatch(self) -> None:
        with pytest.raises(CoreException, match="at least 1ms"):
            _target(apply_max_time=timedelta(0))

        # Sub-millisecond is the *sharper* hole: it passes a "must be positive" check, then
        # reaches the server as int(seconds * 1000) == 0 — and Mongo reads maxTimeMS=0 as
        # unlimited. The tightest-looking setting was the one that turned the bound off.
        for sub_ms in (timedelta(microseconds=1), timedelta(microseconds=999)):
            with pytest.raises(CoreException, match="at least 1ms"):
                _target(apply_max_time=sub_ms)

        # The smallest value that survives conversion is accepted.
        target, _ = _target(apply_max_time=timedelta(milliseconds=1))
        assert target.apply_max_time == timedelta(milliseconds=1)

        # The declared bound covers the command's whole lifetime: the client-side wait
        # before it reaches a server, plus the server-side maxTimeMS.
        target, _ = _target(
            apply_max_time=timedelta(seconds=5),
            dispatch_allowance=timedelta(seconds=10),
        )

        assert target.apply_latency_bound == timedelta(seconds=15)

    async def test_allowance_must_cover_the_clients_configured_dispatch_wait(self) -> None:
        """Validated against what the client is actually configured with, not an estimate —
        and re-checked at apply time, since initialize() can raise it afterwards."""

        secrets = MappingSecrets(data={"db/uri.pending": "mongodb://app_b:pw@db/app"})

        eager = _FakeClient()
        eager.command_dispatch_bound = timedelta(seconds=60)  # type: ignore[attr-defined]

        with pytest.raises(CoreException, match="understates"):
            MongoRotationTarget(
                secrets=secrets,
                client=eager,  # type: ignore[arg-type]
                user_pair=("app_a", "app_b"),
            )

        late = _FakeClient()
        target = MongoRotationTarget(
            secrets=secrets,
            client=late,  # type: ignore[arg-type]
            user_pair=("app_a", "app_b"),
        )
        late.command_dispatch_bound = timedelta(minutes=5)  # type: ignore[attr-defined]

        with pytest.raises(CoreException, match="understates"):
            await target.apply(None, _PENDING)

        assert late.database.commands == []  # refused before any command
