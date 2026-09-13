"""The sensitive-egress gate: a declared egress must be an acknowledged one.

Two fields rather than one, because they are two different statements — the application
declares what a route carries, the operator accepts that it leaves. The tests below pin
that split, and pin the silence of the default: a route that declares nothing is a route
whose wiring did not change.
"""

from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_http.execution.deps import configs
from forze_http.execution.deps.configs import HttpAuthConfig, HttpServiceConfig

pytestmark = pytest.mark.unit

# ----------------------- #


def _config(**overrides: bool) -> HttpServiceConfig:
    return HttpServiceConfig(base_url="https://api.example.com", **overrides)


# ....................... #


class TestTheDefaultIsSilence:
    def test_an_undeclared_route_is_unaffected(self) -> None:
        # The gate must not change any existing wiring: nothing declared, nothing refused.
        config = _config()

        assert config.egress_sensitive is False
        assert config.acknowledge_data_egress is False

    def test_acknowledging_without_declaring_is_not_an_error(self) -> None:
        # Harmless and worth pinning: the acknowledgement is about a declared egress, so
        # an operator who sets it on a route that carries nothing has done nothing wrong.
        # Constructing at all is the assertion — the gate would raise here if it keyed off
        # the acknowledgement rather than off the declaration.
        config = _config(acknowledge_data_egress=True)

        assert config.acknowledge_data_egress is True
        assert config.egress_sensitive is False


# ....................... #


class TestTheGate:
    def test_a_declared_egress_without_acknowledgement_fails_at_wiring(self) -> None:
        with pytest.raises(CoreException) as caught:
            _config(egress_sensitive=True)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "http_egress_unacknowledged"

    def test_the_refusal_names_the_config_and_what_is_being_accepted(self) -> None:
        # An operator reading this has to know what they are being asked to accept.
        with pytest.raises(CoreException) as caught:
            _config(egress_sensitive=True)

        message = caught.value.summary

        assert "HttpServiceConfig" in message
        assert "acknowledge_data_egress=True" in message
        assert "outside the trust boundary" in message

    def test_an_acknowledged_egress_wires(self) -> None:
        config = _config(egress_sensitive=True, acknowledge_data_egress=True)

        assert config.egress_sensitive is True
        assert config.acknowledge_data_egress is True

    def test_the_gate_survives_the_tenant_aware_path(self) -> None:
        # tenant_aware returns early from the rest of the post-init validation, so the
        # gate has to sit where that return cannot skip it.
        with pytest.raises(CoreException) as caught:
            HttpServiceConfig(tenant_aware=True, egress_sensitive=True)

        assert caught.value.code == "http_egress_unacknowledged"


# ....................... #


class TestTheGateIsNotFooledByTruthiness:
    """`attrs` does not enforce annotations, so a field can hold whatever it is given.

    The realistic way that happens is configuration read as text — `os.environ["ACK"]`
    is the string `"false"`, which is perfectly truthy. A gate whose whole purpose is to
    fail closed must not read that as an acknowledgement.
    """

    @pytest.mark.parametrize("value", ["false", "0", "no", 0.0, [], "True"])
    def test_only_a_real_true_acknowledges(self, value: object) -> None:
        with pytest.raises(CoreException) as caught:
            HttpServiceConfig(
                base_url="https://api.example.com",
                egress_sensitive=True,
                acknowledge_data_egress=value,  # type: ignore[arg-type]
            )

        assert caught.value.code == "http_egress_unacknowledged"

    @pytest.mark.parametrize("value", ["false", "0", 1, [1]])
    def test_an_unclear_sensitivity_is_treated_as_sensitive(self, value: object) -> None:
        # The other direction of the same rule: when the declaration cannot be read as a
        # plain False, assume the route carries data out and demand the acknowledgement.
        with pytest.raises(CoreException) as caught:
            HttpServiceConfig(
                base_url="https://api.example.com",
                egress_sensitive=value,  # type: ignore[arg-type]
            )

        assert caught.value.code == "http_egress_unacknowledged"

    def test_a_real_true_still_acknowledges(self) -> None:
        config = _config(egress_sensitive=True, acknowledge_data_egress=True)

        assert config.acknowledge_data_egress is True


# ....................... #


class TestCleartextCredentialWarning:
    """A declared credential over plaintext HTTP is readable on the path. Warned rather
    than refused, because a service mesh legitimately terminates TLS in a sidecar — and
    the warning covers every auth kind, since a bearer token on the wire is as readable as
    a base64 user-and-password."""

    @staticmethod
    def _warnings(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        # The module logger is structlog-backed and writes past caplog, so it is patched
        # directly — the same shape the Mongo index validator's tests use.
        spy = MagicMock()
        monkeypatch.setattr(configs, "logger", spy)

        return spy

    def test_a_credential_over_plaintext_warns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self._warnings(monkeypatch)

        HttpServiceConfig(
            base_url="http://provider.example",
            auth=HttpAuthConfig(kind="basic", username="cid", password="sec"),
        )

        spy.warning.assert_called_once()
        assert "cleartext_credentials" in str(spy.warning.call_args)
        # The credential itself is never in the warning — only the destination and the kind.
        assert "sec" not in str(spy.warning.call_args.kwargs.get("detail", ""))

    def test_a_bearer_token_warns_too(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The finding named Basic; the exposure is identical for every kind, and a guard on
        # one of them would just move the credential to another header.
        spy = self._warnings(monkeypatch)

        HttpServiceConfig(base_url="http://provider.example", auth=HttpAuthConfig(token="t"))

        spy.warning.assert_called_once()

    def test_a_declared_sensitive_route_warns_without_any_auth_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The blind spot a reviewer found: an OAuth token request posts its client secret
        # as a form field and needs no `HttpAuthConfig` at all, so reading `auth` alone
        # stayed silent for exactly the route carrying the most.
        spy = self._warnings(monkeypatch)

        HttpServiceConfig(
            base_url="http://provider.example",
            egress_sensitive=True,
            acknowledge_data_egress=True,
        )

        spy.warning.assert_called_once()
        assert "cleartext_credentials" in str(spy.warning.call_args)

    def test_a_sensitive_route_over_https_is_quiet(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self._warnings(monkeypatch)

        HttpServiceConfig(
            base_url="https://provider.example",
            egress_sensitive=True,
            acknowledge_data_egress=True,
        )

        spy.warning.assert_not_called()

    def test_https_is_quiet(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self._warnings(monkeypatch)

        HttpServiceConfig(base_url="https://provider.example", auth=HttpAuthConfig(token="t"))

        spy.warning.assert_not_called()

    def test_loopback_is_quiet(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A developer's own machine. Warning here trains the reader to ignore the warning.
        spy = self._warnings(monkeypatch)

        # 127.0.0.2 included deliberately: a warning that fires on one loopback address
        # and not another reads as a bug, so the check parses rather than matching spellings.
        for url in (
            "http://localhost:8080",
            "http://127.0.0.1:9000",
            "http://127.0.0.2:9000",
            "http://[::1]:9000",
        ):
            HttpServiceConfig(base_url=url, auth=HttpAuthConfig(token="t"))

        spy.warning.assert_not_called()

    def test_plaintext_without_a_credential_is_quiet(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spy = self._warnings(monkeypatch)

        HttpServiceConfig(base_url="http://provider.example")

        spy.warning.assert_not_called()
