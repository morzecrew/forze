"""Unit tests for :class:`forze_vault.settings.VaultSettings` (no Vault I/O)."""

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("hvac")

from urllib3.util.retry import Retry

from forze_vault._net import is_loopback
from forze_vault.kernel.client import VaultConfig
from forze_vault.kernel.client.client import build_session
from forze_vault.settings import CLIENT_FIELDS, VaultSettings

# ----------------------- #


class TestConfig:
    def test_carries_the_address_and_token_through(self) -> None:
        config = VaultSettings(url="https://vault.internal:8200", token=SecretStr("hvs.x")).config

        assert config.url == "https://vault.internal:8200"
        assert config.token.get_secret_value() == "hvs.x"

    # ....................... #

    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        settings = VaultSettings(url="https://vault.internal")

        assert settings.config == VaultConfig(url="https://vault.internal", token=SecretStr(""))

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = VaultSettings(
            url="https://v",
            namespace="team-a",
            verify=False,
            retry_total=7,
        ).config

        assert (config.namespace, config.verify) == ("team-a", False)
        assert config.retry_total == 7

    # ....................... #

    @pytest.mark.parametrize("url", [None, "", "   "])
    def test_requires_a_url(self, url: str | None) -> None:
        with pytest.raises(CoreException, match="Vault url is required"):
            _ = VaultSettings(url=url).config

    # ....................... #

    @pytest.mark.parametrize("url", ["http://vault.internal:8200", "http://10.0.0.5:8200"])
    def test_refuses_plaintext_to_a_remote_host(self, url: str) -> None:
        """The token rides on every request and every response carries a secret, so
        plaintext to anything but this machine puts both on the wire — and `verify` cannot
        protect a connection that was never encrypted."""

        with pytest.raises(ValidationError, match="must be https"):
            VaultSettings(url=url, token=SecretStr("hvs.x"))

    # ....................... #

    def test_refuses_a_url_with_no_host(self) -> None:
        with pytest.raises(ValidationError, match="must name a host"):
            VaultSettings(url="https://", token=SecretStr("hvs.x"))

    # ....................... #

    def test_refuses_an_out_of_range_port(self) -> None:
        """`urlsplit().port` parses lazily, so it would otherwise sit in the settings
        object until the client read it and reported it as its own problem."""

        with pytest.raises(ValidationError, match="invalid port"):
            VaultSettings(url="https://v:99999", token=SecretStr("hvs.x"))

    # ....................... #

    def test_the_rule_survives_an_assignment(self) -> None:
        """pydantic does not re-run a validator on assignment, so `config` checks it too —
        otherwise the invariant would hold for one instant rather than for the object."""

        settings = VaultSettings(url="https://vault.internal", token=SecretStr("hvs.x"))
        settings.url = "http://vault.internal"

        with pytest.raises(ValueError, match="must be https"):
            _ = settings.config

    # ....................... #

    @pytest.mark.parametrize("url", ["vault.internal", "ftp://v", "ftp://127.0.0.1:8200"])
    def test_refuses_a_url_that_is_not_http_at_all(self, url: str) -> None:
        """Checked before the loopback carve-out, which is why `ftp://127.0.0.1` is
        refused rather than waved through as "loopback, therefore fine"."""

        with pytest.raises(ValidationError, match="must start with https"):
            VaultSettings(url=url, token=SecretStr("hvs.x"))

    # ....................... #

    @pytest.mark.parametrize(
        "url",
        ["http://127.0.0.1:8200", "http://localhost:8200", "http://[::1]:8200"],
    )
    def test_allows_plaintext_to_loopback(self, url: str) -> None:
        """`vault server -dev` listens on `http://127.0.0.1:8200`, and a packet that never
        leaves the machine is not a cleartext transmission."""

        assert VaultSettings(url=url, token=SecretStr("hvs.x")).config.url == url

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `VaultConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(VaultConfig)}
        assert set(CLIENT_FIELDS) <= set(VaultSettings.model_fields)


# ....................... #


class TestSession:
    """`requests` has no built-in localhost bypass, so the plaintext-on-loopback exception
    in `VaultSettings` is only sound if the client stops honouring the proxy environment
    there. This is what makes it sound."""

    @pytest.mark.parametrize("url", ["http://127.0.0.1:8200", "http://localhost:8200"])
    def test_a_loopback_client_ignores_the_proxy_environment(
        self, url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HTTP_PROXY", "http://proxy.corp:8080")
        monkeypatch.delenv("NO_PROXY", raising=False)
        monkeypatch.delenv("no_proxy", raising=False)

        assert build_session(url, retry=Retry(total=1)).trust_env is False

    # ....................... #

    def test_a_remote_client_keeps_normal_proxy_behaviour(self) -> None:
        """A deployment behind a corporate proxy needs it for a remote Vault."""

        assert build_session("https://vault.internal:8200", retry=Retry(total=1)).trust_env

    # ....................... #

    def test_retries_are_mounted_on_both_schemes(self) -> None:
        session = build_session("https://vault.internal", retry=Retry(total=1))

        assert set(session.adapters) >= {"http://", "https://"}


# ....................... #


class TestIsLoopback:
    """Shared by the settings validator and the client's session, and reachable from the
    client with no host — `build_session` has no `_checked_url` in front of it."""

    @pytest.mark.parametrize(
        "hostname",
        ["localhost", "127.0.0.1", "127.1.2.3", "::1", "[::1]"],
    )
    def test_recognises_this_machine(self, hostname: str) -> None:
        assert is_loopback(hostname) is True

    # ....................... #

    @pytest.mark.parametrize("hostname", ["::ffff:127.0.0.1", "[::ffff:127.0.0.1]"])
    def test_recognises_an_ipv4_mapped_loopback(self, hostname: str) -> None:
        """Leaning on the stdlib rather than restating it: `IPv6Address.is_loopback`
        delegates to the mapped IPv4 address from 3.13, which is this project's floor. The
        test is here because that is a version-dependent guarantee a security decision
        rests on — if it ever stops holding, this fails rather than the token quietly
        starting to travel through a proxy."""

        assert is_loopback(hostname) is True

    # ....................... #

    @pytest.mark.parametrize(
        "hostname",
        [None, "", "vault.internal", "10.0.0.5", "2001:db8::1", "localhost.evil.com"],
    )
    def test_recognises_everything_else(self, hostname: str | None) -> None:
        assert is_loopback(hostname) is False
