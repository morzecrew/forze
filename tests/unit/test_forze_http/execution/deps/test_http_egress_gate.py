"""The sensitive-egress gate: a declared egress must be an acknowledged one.

Two fields rather than one, because they are two different statements — the application
declares what a route carries, the operator accepts that it leaves. The tests below pin
that split, and pin the silence of the default: a route that declares nothing is a route
whose wiring did not change.
"""

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_http.execution.deps.configs import HttpServiceConfig

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
