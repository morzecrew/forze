"""Secrets lifecycle contracts: capabilities fail closed, values never leak via repr."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.secrets import (
    DEFAULT_SECRETS_CAPABILITIES,
    FULL_SECRETS_CAPABILITIES,
    LeasedSecret,
    SecretsCapabilities,
    SecretValue,
    SecretVersion,
    content_secret_version,
    secrets_capabilities_of,
    validate_dynamic_credentials_supported,
    validate_secret_writes_supported,
    validate_versioned_reads_supported,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class TestSecretsCapabilities:
    def test_defaults_are_all_off(self) -> None:
        assert DEFAULT_SECRETS_CAPABILITIES == SecretsCapabilities()
        assert not DEFAULT_SECRETS_CAPABILITIES.versioned_reads
        assert not DEFAULT_SECRETS_CAPABILITIES.writes

    def test_full_surface_is_all_on(self) -> None:
        assert FULL_SECRETS_CAPABILITIES.versioned_reads
        assert FULL_SECRETS_CAPABILITIES.native_versions
        assert FULL_SECRETS_CAPABILITIES.writes
        assert FULL_SECRETS_CAPABILITIES.change_feed
        assert FULL_SECRETS_CAPABILITIES.dynamic_credentials

    def test_native_versions_require_versioned_reads(self) -> None:
        with pytest.raises(CoreException, match="native_versions"):
            SecretsCapabilities(native_versions=True)

    def test_validators_fail_closed_with_named_code(self) -> None:
        caps = DEFAULT_SECRETS_CAPABILITIES

        for validate in (
            validate_versioned_reads_supported,
            validate_secret_writes_supported,
            validate_dynamic_credentials_supported,
        ):
            with pytest.raises(CoreException, match="not supported") as excinfo:
                validate(caps, backend="test-backend")

            assert excinfo.value.code == "secrets_feature_unsupported"
            assert "test-backend" in excinfo.value.summary

    def test_validators_pass_on_capable_backend(self) -> None:
        caps = FULL_SECRETS_CAPABILITIES

        validate_versioned_reads_supported(caps, backend="b")
        validate_secret_writes_supported(caps, backend="b")
        validate_dynamic_credentials_supported(caps, backend="b")

    def test_capabilities_of_defaults_for_plain_adapters(self) -> None:
        class _Plain:
            pass

        assert secrets_capabilities_of(_Plain()) == DEFAULT_SECRETS_CAPABILITIES

    def test_capabilities_of_reads_the_declared_property(self) -> None:
        class _Declared:
            @property
            def secrets_capabilities(self) -> SecretsCapabilities:
                return SecretsCapabilities(versioned_reads=True)

        assert secrets_capabilities_of(_Declared()).versioned_reads


# ....................... #


class TestValueHygiene:
    def test_secret_value_repr_hides_the_text(self) -> None:
        value = SecretValue(text="postgres://user:hunter2@db/x", version=SecretVersion("v1"))

        assert "hunter2" not in repr(value)
        assert "v1" in repr(value)

    def test_leased_secret_repr_hides_the_text(self) -> None:
        leased = LeasedSecret(
            text='{"password": "hunter2"}',
            lease_id="db/creds/abc",
            ttl=timedelta(seconds=60),
            renewable=True,
        )

        assert "hunter2" not in repr(leased)
        assert "db/creds/abc" in repr(leased)

    def test_content_version_is_deterministic_and_content_bound(self) -> None:
        assert content_secret_version("a") == content_secret_version("a")
        assert content_secret_version("a") != content_secret_version("b")
