"""Tests for typed object-store credentials -> CREATE SECRET rendering + validation."""

from __future__ import annotations

import pytest

from forze.application.contracts.secrets import SecretRef
from forze.base.exceptions import CoreException
from forze_duckdb import (
    GcsCredentials,
    GcsSecretPayload,
    S3Credentials,
    S3SecretPayload,
)

# ----------------------- #


def test_s3_inline_renders_create_secret() -> None:
    cred = S3Credentials(
        name="lake",
        inline=S3SecretPayload(
            access_key_id="AK",
            secret_access_key="SK",  # type: ignore[arg-type]
            region="eu-west-1",
            endpoint="minio:9000",
            url_style="path",
            use_ssl=False,
        ),
    )

    stmt = cred.render(cred.inline_payload())  # type: ignore[arg-type]

    assert stmt.startswith("CREATE OR REPLACE SECRET lake (")
    assert "TYPE S3" in stmt
    assert "KEY_ID 'AK'" in stmt
    assert "SECRET 'SK'" in stmt
    assert "REGION 'eu-west-1'" in stmt
    assert "ENDPOINT 'minio:9000'" in stmt
    assert "URL_STYLE 'path'" in stmt
    assert "USE_SSL false" in stmt
    assert cred.required_extensions() == ("httpfs",)


# ....................... #


def test_s3_scope_rendered() -> None:
    cred = S3Credentials(
        name="lake",
        scope="s3://bucket",
        inline=S3SecretPayload(access_key_id="AK", secret_access_key="SK"),  # type: ignore[arg-type]
    )

    assert "SCOPE 's3://bucket'" in cred.render(cred.inline_payload())  # type: ignore[arg-type]


# ....................... #


def test_gcs_inline_renders_create_secret() -> None:
    cred = GcsCredentials(
        name="gcslake",
        inline=GcsSecretPayload(key_id="KID", secret="SEC"),  # type: ignore[arg-type]
    )

    stmt = cred.render(cred.inline_payload())  # type: ignore[arg-type]

    assert stmt.startswith("CREATE OR REPLACE SECRET gcslake (")
    assert "TYPE GCS" in stmt
    assert "KEY_ID 'KID'" in stmt
    assert "SECRET 'SEC'" in stmt


# ....................... #


def test_exactly_one_of_inline_or_secret_ref_required() -> None:
    with pytest.raises(CoreException, match="exactly one"):
        S3Credentials(name="x")  # neither

    with pytest.raises(CoreException, match="exactly one"):
        S3Credentials(
            name="x",
            inline=S3SecretPayload(access_key_id="A", secret_access_key="B"),  # type: ignore[arg-type]
            secret_ref=SecretRef(path="p"),
        )  # both


# ....................... #


def test_secret_ref_form_has_no_inline_payload() -> None:
    cred = S3Credentials(name="lake", secret_ref=SecretRef(path="lake/s3"))

    assert cred.inline_payload() is None
    assert cred.payload_model() is S3SecretPayload


# ....................... #


def test_name_must_be_bare_identifier() -> None:
    with pytest.raises(CoreException, match="bare SQL identifier"):
        S3Credentials(
            name="bad-name",
            inline=S3SecretPayload(access_key_id="A", secret_access_key="B"),  # type: ignore[arg-type]
        )


# ....................... #


def test_secret_value_not_in_repr() -> None:
    cred = S3Credentials(
        name="lake",
        inline=S3SecretPayload(access_key_id="AK", secret_access_key="topsecret"),  # type: ignore[arg-type]
    )

    # The attrs `inline` field is repr=False and SecretStr masks the value anyway.
    assert "topsecret" not in repr(cred)


# ....................... #


def test_render_rejects_wrong_payload_type() -> None:
    cred = S3Credentials(name="lake", secret_ref=SecretRef(path="p"))

    with pytest.raises(CoreException, match="S3SecretPayload"):
        cred.render(GcsSecretPayload(key_id="K", secret="S"))  # type: ignore[arg-type]
