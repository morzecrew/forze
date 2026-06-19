"""Tests for :mod:`forze.base.primitives.fingerprint`."""

import pytest
from pydantic import SecretStr

from forze.base.primitives.fingerprint import (
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


def test_stable_json_bytes_falls_back_to_str() -> None:
    # Non-JSON-native values are coerced via ``default=str`` rather than raising.
    assert stable_json_bytes({"x": object}) == stable_json_bytes({"x": object})


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
