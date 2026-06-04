"""Tests for :mod:`forze.base.primitives.fingerprint`."""

from pydantic import SecretStr

from forze.base.primitives.fingerprint import (
    build_routing_fingerprint,
    connection_string_fingerprint,
    gcp_credential_dedup_tag,
    secret_dedup_fingerprint,
    stable_fingerprint,
)
from forze_clickhouse.kernel.client.routing_credentials import (
    ClickHouseRoutingCredentials,
    routing_fingerprint,
)

# ----------------------- #


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
