"""Tests for :mod:`forze.base.primitives.fingerprint`."""

import os
import subprocess
import sys
import textwrap
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from enum import Enum, StrEnum
from typing import NamedTuple
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives.fingerprint import (
    advisory_lock_key,
    build_routing_fingerprint,
    connection_string_fingerprint,
    gcp_credential_dedup_tag,
    secret_dedup_fingerprint,
    stable_fingerprint,
    stable_json_bytes,
    stable_payload_fingerprint,
)
from forze_clickhouse.kernel.client.routing_credentials import (
    ClickHouseRoutingCredentials,
    routing_fingerprint,
)

# ----------------------- #


def test_stable_json_bytes_is_key_order_independent() -> None:
    assert stable_json_bytes({"a": 1, "b": 2}) == stable_json_bytes({"b": 2, "a": 1})


def test_stable_json_bytes_sorts_sets() -> None:
    """A set has no order, so a canonical form must impose one.

    orjson cannot serialize a ``set``, so without this it lands in ``default``, is rendered as
    ``str(some_set)`` — iteration order — and **string hashing is seeded per process**. The same
    payload would fingerprint differently in the next interpreter. See
    ``test_hash_args_is_identical_across_processes``, which is the test that can actually observe
    that; this one only pins the rendering.
    """

    assert stable_json_bytes({"s": {"b", "a", "c"}}) == b'{"s":["a","b","c"]}'
    assert stable_json_bytes({"s": frozenset({"b", "a"})}) == stable_json_bytes({"s": {"a", "b"}})


def test_stable_json_bytes_sorts_a_mixed_type_set_without_raising() -> None:
    """``sorted`` on mixed types raises; a fingerprint may not. The order is canonical, not
    semantic — it exists so two processes agree, not so a human approves."""

    once = stable_json_bytes({"s": {1, "a", 2.5}})

    assert once == stable_json_bytes({"s": {2.5, 1, "a"}})


def test_stable_json_bytes_sorts_nested_sets() -> None:
    """A set *of sets* — where sorting the outer one by each element's repr reintroduces the very
    bug it closes, one level down.

    ``str(frozenset)`` renders in iteration order too, so a repr-keyed sort has a **seed-dependent
    sort key** and the outer order flips between processes even though each inner set is sorted.
    Sorting by canonical bytes recurses, so everything below is already ordered when compared.
    See ``test_nested_sets_hash_identically_across_processes`` — this only pins the rendering.
    """

    nested = {frozenset({"zulu", "alpha"}), frozenset({"charlie", "bravo"})}

    assert stable_json_bytes({"s": nested}) == b'{"s":[["alpha","zulu"],["bravo","charlie"]]}'


def test_nested_sets_hash_identically_across_processes() -> None:
    """The test that can fail for a real reason: a fingerprint is compared *between* processes,
    so anything hash-seeded is invisible to a test that computes it once."""

    script = textwrap.dedent(
        """
        from forze.base.primitives import stable_payload_fingerprint

        print(
            stable_payload_fingerprint(
                {
                    "outer": {
                        frozenset({"alpha", "zulu"}),
                        frozenset({"bravo", "charlie"}),
                        frozenset({"delta", "echo", "foxtrot"}),
                    }
                }
            )
        )
        """
    )

    digests = {
        seed: subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        for seed in ("0", "1", "7", "12345")
    }

    assert len(set(digests.values())) == 1, f"nested-set fingerprint varies by seed: {digests}"


def test_stable_json_bytes_falls_back_to_str() -> None:
    # Non-JSON-native values are coerced via ``default=str`` rather than raising.
    assert stable_json_bytes({"x": object}) == stable_json_bytes({"x": object})


class _Name(StrEnum):
    ORDERS = "orders"


def test_a_str_enum_key_hashes_as_its_value_at_any_depth() -> None:
    """A spec or member name may be a ``StrEnum``; keyed by one, a payload is still a string-keyed
    payload, and fingerprints exactly as the plain string would."""

    assert stable_json_bytes({_Name.ORDERS: 1}) == stable_json_bytes({"orders": 1})
    assert stable_json_bytes({"x": [{"y": {_Name.ORDERS: [1]}}]}) == stable_json_bytes(
        {"x": [{"y": {"orders": [1]}}]}
    )


class _MixedName(str, Enum):
    """The ``(str, Enum)`` spelling, whose ``str()`` is ``_MixedName.ORDERS``, not its value."""

    ORDERS = "orders"


class _Point(NamedTuple):
    x: int
    y: int


def test_a_string_key_hashes_as_its_text_whatever_its_str_returns() -> None:
    assert stable_json_bytes({_MixedName.ORDERS: 1}) == stable_json_bytes({"orders": 1})


def test_a_str_enum_key_leaves_the_values_as_the_plain_key_would() -> None:
    """Only keys change: a value orjson renders through ``str`` (a named tuple) renders the same
    under a ``StrEnum`` key as under the plain one."""

    point = _Point(1, 2)

    assert stable_json_bytes({_Name.ORDERS: point}) == stable_json_bytes({"orders": point})


def test_a_key_that_is_not_a_string_is_still_refused() -> None:
    """Only string keys: an ``int`` key would fingerprint as its digits, the same as a ``"1"``
    key, and two different payloads would share a fingerprint."""

    with pytest.raises(TypeError):
        stable_json_bytes({1: "a"})


def test_stable_payload_fingerprint_is_deterministic_and_prefixed() -> None:
    fp = stable_payload_fingerprint({"b": 2, "a": 1})

    assert fp == stable_payload_fingerprint({"a": 1, "b": 2})
    assert fp.startswith("sha256:")


def test_stable_payload_fingerprint_differs_by_content() -> None:
    assert stable_payload_fingerprint({"a": 1}) != stable_payload_fingerprint({"a": 2})


def test_stable_payload_fingerprint_bare_digest_when_no_prefix() -> None:
    bare = stable_payload_fingerprint({"a": 1}, prefix="")

    assert ":" not in bare
    assert len(bare) == 64  # sha256 hex


def test_stable_fingerprint_is_deterministic() -> None:
    assert stable_fingerprint("a", "b") == stable_fingerprint("a", "b")


def test_stable_fingerprint_differs_for_different_parts() -> None:
    assert stable_fingerprint("a") != stable_fingerprint("b")


def test_secret_dedup_fingerprint_is_deterministic() -> None:
    assert secret_dedup_fingerprint("key") == secret_dedup_fingerprint("key")


def test_secret_dedup_fingerprint_empty_and_none() -> None:
    assert secret_dedup_fingerprint(None) == ""
    assert secret_dedup_fingerprint("") == ""
    assert secret_dedup_fingerprint(SecretStr("")) == ""


def test_secret_dedup_fingerprint_differs_for_different_values() -> None:
    assert secret_dedup_fingerprint("a") != secret_dedup_fingerprint("b")


def test_secret_dedup_fingerprint_accepts_secret_str() -> None:
    assert secret_dedup_fingerprint(SecretStr("key")) == secret_dedup_fingerprint("key")


def test_gcp_credential_dedup_tag_file() -> None:
    assert gcp_credential_dedup_tag(service_file="/path/key.json") == "file:/path/key.json"


def test_gcp_credential_dedup_tag_inline_never_embeds_raw_json() -> None:
    raw = '{"client_email":"x@y.z"}'
    tag = gcp_credential_dedup_tag(service_account_json=raw)

    assert tag.startswith("inline:")
    assert raw not in tag


def test_gcp_credential_dedup_tag_adc() -> None:
    assert gcp_credential_dedup_tag() == "adc"


def test_connection_string_fingerprint_differs_by_password() -> None:
    base = "postgresql://user"
    host = "@localhost:5432/mydb"
    fp_a = connection_string_fingerprint(f"{base}:pass-a{host}")
    fp_b = connection_string_fingerprint(f"{base}:pass-b{host}")

    assert fp_a != fp_b
    assert "pass-a" not in fp_a
    assert "pass-b" not in fp_b


def test_connection_string_fingerprint_without_password_unchanged_shape() -> None:
    dsn = "postgresql://user@localhost:5432/mydb"
    fp = connection_string_fingerprint(dsn)

    assert fp
    assert "user" not in fp


def test_connection_string_fingerprint_differs_by_query_params() -> None:
    base = "127.0.0.1:7233"
    fp_a = connection_string_fingerprint(base)
    fp_b = connection_string_fingerprint(f"{base}?dedup=aaa")
    fp_c = connection_string_fingerprint(f"{base}?dedup=bbb")

    assert fp_a != fp_b != fp_c


@pytest.mark.parametrize(
    "dsn",
    [
        # Multi-host authorities (replica set / sentinel / cluster) carry a
        # comma-separated host list that ``urlparse(...).port`` cannot parse.
        "mongodb://u:topsecret@h1:27017,h2:27017,h3:27017/db?replicaSet=rs0",
        "redis://h1:26379,h2:26379,h3:26379/0",
        "amqp://u:topsecret@h1:5672,h2:5672/vhost",
        # Single-host forms (incl. IPv6) must keep working.
        "postgresql://u:topsecret@host:5432/db?sslmode=require",
        "postgresql://u:topsecret@[::1]:5432/db",
    ],
)
def test_connection_string_fingerprint_handles_multi_host_dsn(dsn: str) -> None:
    # Regression: must not raise ValueError on a comma-separated host list,
    # and the raw password must never appear in the fingerprint.
    fp = connection_string_fingerprint(dsn)
    assert fp
    assert "topsecret" not in fp


def test_connection_string_fingerprint_distinguishes_host_sets() -> None:
    # Two DSNs differing only in a non-first host must not collide (the old
    # code fingerprinted only ``parsed.hostname`` = the first host).
    a = connection_string_fingerprint("mongodb://u:p@h1:27017,h2:27017/db")
    b = connection_string_fingerprint("mongodb://u:p@h1:27017,h9:27017/db")
    assert a != b


def test_clickhouse_routing_fingerprint_differs_by_password() -> None:
    creds_a = ClickHouseRoutingCredentials(password="secret-a")
    creds_b = ClickHouseRoutingCredentials(password="secret-b")

    fp_a = routing_fingerprint(creds_a)
    fp_b = routing_fingerprint(creds_b)

    assert fp_a != fp_b


def test_clickhouse_routing_fingerprint_excludes_plaintext_password() -> None:
    password = "super-secret-password"
    creds = ClickHouseRoutingCredentials(password=password)
    fp = routing_fingerprint(creds)

    assert password not in fp


# ....................... #


def test_build_routing_fingerprint_is_deterministic() -> None:
    a = build_routing_fingerprint(public=["host", "5432"], secret=["pw"])
    b = build_routing_fingerprint(public=["host", "5432"], secret=["pw"])

    assert a == b


def test_build_routing_fingerprint_differs_by_public_field() -> None:
    a = build_routing_fingerprint(public=["host-a"], secret=["pw"])
    b = build_routing_fingerprint(public=["host-b"], secret=["pw"])

    assert a != b


def test_build_routing_fingerprint_detects_secret_rotation() -> None:
    a = build_routing_fingerprint(public=["host"], secret=["pw-a"])
    b = build_routing_fingerprint(public=["host"], secret=["pw-b"])

    assert a != b


def test_build_routing_fingerprint_ignores_empty_secrets() -> None:
    base = build_routing_fingerprint(public=["host"])

    assert base == build_routing_fingerprint(public=["host"], secret=[None])
    assert base == build_routing_fingerprint(public=["host"], secret=[""])
    assert base == build_routing_fingerprint(public=["host"], secret=[None, ""])


def test_build_routing_fingerprint_presence_of_secret_changes_key() -> None:
    assert build_routing_fingerprint(public=["host"]) != build_routing_fingerprint(
        public=["host"],
        secret=["pw"],
    )


def test_build_routing_fingerprint_never_embeds_plaintext_secret() -> None:
    secret = "super-secret-value"
    fp = build_routing_fingerprint(public=["host"], secret=[secret])

    assert secret not in fp


def test_build_routing_fingerprint_accepts_secret_str() -> None:
    assert build_routing_fingerprint(
        public=["h"],
        secret=[SecretStr("pw")],
    ) == build_routing_fingerprint(public=["h"], secret=["pw"])


# ....................... #

_NOON_IN_MOSCOW = datetime(2026, 3, 29, 12, 0, tzinfo=timezone(timedelta(hours=3)))
"""09:00 UTC, as a caller in ``+03:00`` writes it."""


class TestAnInstantIsOneLockKeyInAnyOffset:
    """A store reads an instant back in its session's offset, not in the one it was written with.

    Postgres returns a ``timestamptz`` in the session's time zone, so a create keyed on the
    caller's ``12:00+03:00`` and an update keyed on the row read back as ``09:00+00:00`` (or
    ``18:00+09:00``) must derive one key, or the two writes for one owner do not contend.
    """

    @pytest.mark.parametrize(
        "zone",
        [
            timezone(timedelta(hours=3)),
            timezone(timedelta(hours=-5)),
            timezone(timedelta(hours=5, minutes=45)),
            timezone(timedelta(hours=14)),
            timezone(timedelta(hours=-12)),
            ZoneInfo("Asia/Tokyo"),
            ZoneInfo("America/New_York"),
        ],
        ids=str,
    )
    def test_the_same_instant_in_another_offset_is_the_same_key(self, zone: tzinfo) -> None:
        assert advisory_lock_key("s", None, _NOON_IN_MOSCOW.astimezone(zone)) == (
            advisory_lock_key("s", None, _NOON_IN_MOSCOW)
        )

    def test_the_same_wall_time_in_another_offset_is_another_key(self) -> None:
        # The contrast: it is the instant that is keyed, not the digits.
        assert advisory_lock_key("s", None, _NOON_IN_MOSCOW) != advisory_lock_key(
            "s", None, _NOON_IN_MOSCOW.replace(tzinfo=UTC)
        )

    def test_a_naive_datetime_is_the_utc_instant(self) -> None:
        # As forze reads a naive datetime everywhere else, and as a UTC session stores one.
        assert advisory_lock_key("s", None, datetime(2026, 3, 29, 9, 0)) == advisory_lock_key(
            "s", None, _NOON_IN_MOSCOW
        )

    def test_microseconds_are_part_of_the_instant(self) -> None:
        later = _NOON_IN_MOSCOW + timedelta(microseconds=1)

        assert advisory_lock_key("s", None, later) != advisory_lock_key("s", None, _NOON_IN_MOSCOW)
        assert advisory_lock_key("s", None, later.astimezone(UTC)) == advisory_lock_key(
            "s", None, later
        )

    def test_the_repeated_hour_at_a_dst_change_is_two_instants(self) -> None:
        # 02:30 happens twice in Berlin on 2026-10-25: first in CEST (00:30 UTC), then in CET
        # (01:30 UTC). Each is its own owner, and each is the owner its UTC spelling names.
        berlin = ZoneInfo("Europe/Berlin")
        first = datetime(2026, 10, 25, 2, 30, tzinfo=berlin, fold=0)
        second = datetime(2026, 10, 25, 2, 30, tzinfo=berlin, fold=1)

        assert advisory_lock_key("s", None, first) != advisory_lock_key("s", None, second)
        assert advisory_lock_key("s", None, first) == advisory_lock_key(
            "s", None, datetime(2026, 10, 25, 0, 30, tzinfo=UTC)
        )
        assert advisory_lock_key("s", None, second) == advisory_lock_key(
            "s", None, datetime(2026, 10, 25, 1, 30, tzinfo=UTC)
        )

    @pytest.mark.parametrize(
        ("value", "offset"),
        [
            (datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))), "-01:00"),
            (datetime.min.replace(tzinfo=timezone(timedelta(hours=5, minutes=30))), "+05:30"),
        ],
        ids=["after-9999", "before-1"],
    )
    def test_an_instant_outside_the_utc_range_is_refused(
        self, value: datetime, offset: str
    ) -> None:
        # Its UTC spelling is no `datetime`, so there is no key to derive: refused as the
        # caller's value, naming the offset rather than echoing the value.
        with pytest.raises(CoreException) as caught:
            advisory_lock_key("s", None, value)

        assert caught.value.kind is ExceptionKind.VALIDATION
        assert caught.value.code == "lock_key_instant_out_of_range"
        assert offset in caught.value.summary
        assert caught.value.details == {"offset": offset}
        for echoed in (value.isoformat(), value.date().isoformat(), value.time().isoformat()):
            assert echoed not in caught.value.summary
            assert echoed not in str(caught.value.details)


class TestEveryOtherLockKeyIsUnchanged:
    """Values pinned to the keys they derived before instants were keyed in UTC.

    A lock key that shifts between releases is a rolling deployment where old and new nodes do
    not contend, so a change to the derivation must not reach any key it was not meant to. An
    aware instant already in UTC renders as it always did, so its key is pinned too.
    """

    @pytest.mark.parametrize(
        ("parts", "key"),
        [
            (("bookings", None, "owner", "o1"), -6374164214799014836),
            (("bookings", "t1", "owner", 42), -7723614124364265392),
            (("bookings", None, "owner", True), 1142358895311863153),
            (("bookings", None, "owner", b"o1"), -4517180046794002836),
            (
                ("bookings", None, "owner", UUID("12345678-1234-5678-1234-567812345678")),
                -6592787601288015191,
            ),
            (("bookings", None, "owner", Decimal("1.50")), 4001118793811261361),
            (("bookings", None, "day", date(2026, 3, 29)), 6216952544602733436),
            (("bookings", None, "owner", None), 1775724267125941704),
            (("bookings", "t1", "owner", "room", "o1", -7), 8747620385631718901),
            (
                ("bookings", None, "slot", datetime(2026, 3, 29, 9, 0, tzinfo=UTC)),
                -2189144401023838394,
            ),
            (
                ("bookings", None, "slot", datetime(2026, 3, 29, 9, 0, 0, 123456, tzinfo=UTC)),
                -3732741677714925686,
            ),
        ],
    )
    def test_the_key_is_what_it_was(self, parts: tuple[object, ...], key: int) -> None:
        assert advisory_lock_key(*parts) == key
