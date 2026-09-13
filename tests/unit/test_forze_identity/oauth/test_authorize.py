"""The authorize-URL builder: pure, and refusing the mistakes that make a callback unsafe."""

from urllib.parse import parse_qs, urlsplit

import pytest

from forze.base.exceptions import CoreException
from forze_identity.oauth import build_authorize_url, generate_pkce, generate_state

pytestmark = pytest.mark.unit

# ----------------------- #

_ENDPOINT = "https://provider.example/oauth/authorize"


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


# ....................... #


class TestTheBuiltUrl:
    def test_it_carries_the_code_flow_parameters(self) -> None:
        url = build_authorize_url(
            _ENDPOINT,
            client_id="cid",
            redirect_uri="https://app.example/cb",
            state="the-state",
        )
        params = _query(url)

        assert params["response_type"] == ["code"]
        assert params["client_id"] == ["cid"]
        assert params["redirect_uri"] == ["https://app.example/cb"]
        assert params["state"] == ["the-state"]
        # No scope key at all rather than an empty one: an empty `scope=` is a request for
        # nothing, where omitting it asks the provider for its default grant.
        assert "scope" not in params

    def test_scopes_are_space_delimited_and_percent_encoded(self) -> None:
        url = build_authorize_url(
            _ENDPOINT,
            client_id="cid",
            redirect_uri="https://app.example/cb",
            state="s",
            scopes=["crm.read", "crm.write"],
        )

        # %20 rather than "+": in a query string "+" means space only by form-encoding
        # convention, and a provider reading it literally grants a scope nobody asked for.
        assert "scope=crm.read%20crm.write" in url
        assert _query(url)["scope"] == ["crm.read crm.write"]

    def test_pkce_declares_s256_only_when_a_challenge_is_given(self) -> None:
        pair = generate_pkce()
        with_pkce = _query(
            build_authorize_url(
                _ENDPOINT,
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
                code_challenge=pair.code_challenge,
            )
        )
        without = _query(
            build_authorize_url(
                _ENDPOINT, client_id="cid", redirect_uri="https://app.example/cb", state="s"
            )
        )

        assert with_pkce["code_challenge"] == [pair.code_challenge]
        assert with_pkce["code_challenge_method"] == ["S256"]
        # `plain` is never built: it offers nothing against an intercepted code, and a
        # method parameter with no challenge would be a lie.
        assert "code_challenge_method" not in without
        assert "code_challenge" not in without

    def test_the_verifier_never_appears(self) -> None:
        # The one thing that must not travel with the redirect. Asserted rather than
        # assumed, because it sits one field away from the challenge that must.
        pair = generate_pkce()
        url = build_authorize_url(
            _ENDPOINT,
            client_id="cid",
            redirect_uri="https://app.example/cb",
            state=generate_state(),
            code_challenge=pair.code_challenge,
        )

        assert pair.code_verifier not in url

    def test_an_endpoints_own_query_survives(self) -> None:
        # A provider that publishes a parameter in its endpoint (a tenant, an API version)
        # keeps it; appending must not replace what was already there.
        url = build_authorize_url(
            "https://provider.example/oauth/authorize?tenant=7",
            client_id="cid",
            redirect_uri="https://app.example/cb",
            state="s",
        )
        params = _query(url)

        assert params["tenant"] == ["7"]
        assert params["client_id"] == ["cid"]

    def test_extra_params_are_merged(self) -> None:
        url = build_authorize_url(
            _ENDPOINT,
            client_id="cid",
            redirect_uri="https://app.example/cb",
            state="s",
            extra_params={"prompt": "consent", "access_type": "offline"},
        )
        params = _query(url)

        assert params["prompt"] == ["consent"]
        assert params["access_type"] == ["offline"]


class TestTransportSecurity:
    """OAuth 2.1 asks for TLS on both ends; RFC 8252 carves out loopback for native apps."""

    def test_a_plaintext_endpoint_is_refused(self) -> None:
        # A non-loopback http endpoint puts the authorization code on the wire in clear,
        # and a code is all an interceptor needs whenever PKCE is absent.
        with pytest.raises(CoreException) as raised:
            build_authorize_url(
                "http://provider.example/oauth/authorize",
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
            )

        assert "https" in str(raised.value)

    def test_a_plaintext_redirect_is_refused(self) -> None:
        with pytest.raises(CoreException):
            build_authorize_url(
                _ENDPOINT, client_id="cid", redirect_uri="http://app.example/cb", state="s"
            )

    def test_a_relative_redirect_is_refused(self) -> None:
        # The provider needs somewhere to send the user; a path is not somewhere.
        with pytest.raises(CoreException):
            build_authorize_url(_ENDPOINT, client_id="cid", redirect_uri="/cb", state="s")

    @pytest.mark.parametrize(
        "endpoint",
        ["http://localhost:8080/auth", "http://127.0.0.1:9000/auth", "http://[::1]:9000/auth"],
    )
    def test_an_http_loopback_endpoint_is_allowed(self, endpoint: str) -> None:
        # A local IdP in a container is how this is developed against, and refusing it
        # would push people to disable the check rather than to use TLS.
        assert build_authorize_url(
            endpoint, client_id="cid", redirect_uri="https://app.example/cb", state="s"
        ).startswith(endpoint)

    @pytest.mark.parametrize("redirect", ["http://localhost:3000/cb", "http://127.0.0.2:3000/cb"])
    def test_an_http_loopback_redirect_is_allowed(self, redirect: str) -> None:
        # RFC 8252's native-app case, and 127.0.0.2 is as loopback as 127.0.0.1.
        assert redirect.split(":")[1].lstrip("/") in build_authorize_url(
            _ENDPOINT, client_id="cid", redirect_uri=redirect, state="s"
        )


class TestWhatItRefuses:
    def test_a_malformed_url_is_a_refusal_not_a_value_error(self) -> None:
        # `urlsplit` raises on an unclosed IPv6 bracket. A bare ValueError escaping a
        # validator breaks its documented contract and turns a 400 into a 500.
        for bad in ("https://[oops/path", "https://app.example/cb"):
            args = {
                "client_id": "cid",
                "redirect_uri": bad,
                "state": "s",
            }

            try:
                build_authorize_url(_ENDPOINT, **args)

            except CoreException:
                pass

            except ValueError as leaked:  # pragma: no cover - the defect this pins
                raise AssertionError(f"a ValueError escaped for {bad!r}") from leaked

        with pytest.raises(CoreException):
            build_authorize_url(
                "https://[oops/path",
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
            )

    def test_a_relative_endpoint(self) -> None:
        with pytest.raises(CoreException):
            build_authorize_url(
                "/oauth/authorize",
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
            )

    def test_a_non_http_scheme(self) -> None:
        # An `app://` or `javascript:` endpoint is a redirect target, not an authorization
        # server.
        with pytest.raises(CoreException):
            build_authorize_url(
                "javascript:alert(1)",
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
            )

    def test_an_empty_state(self) -> None:
        # An empty state compares equal to an empty session value, which is how a callback
        # with no session at all would pass the check the state exists for.
        with pytest.raises(CoreException):
            build_authorize_url(
                _ENDPOINT, client_id="cid", redirect_uri="https://app.example/cb", state=""
            )

    @pytest.mark.parametrize(
        "owned", ["state", "redirect_uri", "client_id", "response_type", "scope"]
    )
    def test_an_endpoint_that_publishes_an_owned_parameter(self, owned: str) -> None:
        # The other side of the override: appending beside a published `state=` duplicates
        # the parameter, and which one a provider honours is unspecified — first, last, or
        # a rejected request. The existing test that an endpoint's own query survives is
        # about parameters nobody here owns.
        with pytest.raises(CoreException) as raised:
            build_authorize_url(
                f"https://provider.example/oauth/authorize?{owned}=attacker",
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
            )

        assert owned in str(raised.value)

    @pytest.mark.parametrize(
        "owned",
        ["client_id", "redirect_uri", "response_type", "scope", "state", "code_challenge"],
    )
    def test_an_extra_param_that_overrides_an_owned_one(self, owned: str) -> None:
        # The sharp case is `redirect_uri`: a caller passing its own through extra_params
        # believes it changed where the user comes back to, and a silent last-write-wins
        # would send one value while the token exchange presents another.
        with pytest.raises(CoreException) as raised:
            build_authorize_url(
                _ENDPOINT,
                client_id="cid",
                redirect_uri="https://app.example/cb",
                state="s",
                extra_params={owned: "https://evil.example"},
            )

        assert owned in str(raised.value)
