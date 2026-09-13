"""Authorization-endpoint URL construction for OAuth 2.1 / OIDC authorization-code flows.

Pure and direction-neutral: the same URL starts an inbound login and an outbound
"connect this account" flow, and only what the callback *does* with the code differs. So
it lives beside :mod:`~forze_identity.oauth.pkce` and :mod:`~forze_identity.oauth.state`
rather than in whichever package happened to need it first, and it does no I/O — the
caller keeps the ``state`` and the PKCE verifier in its session, which is the storage
doctrine those two modules already follow.
"""

from collections.abc import Iterable, Mapping
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from forze.base.exceptions import exc

# ----------------------- #

_RESERVED: frozenset[str] = frozenset(
    {
        "client_id",
        "code_challenge",
        "code_challenge_method",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
    }
)
"""Parameters this function owns. An extra that repeats one of them is refused rather
than silently overriding it — a caller that passes its own ``redirect_uri`` through
*extra_params* believes it changed the request, and the one sent would be the other."""


# ....................... #


def build_authorize_url(
    authorization_endpoint: str,
    *,
    client_id: str,
    redirect_uri: str,
    state: str,
    scopes: Iterable[str] = (),
    code_challenge: str | None = None,
    extra_params: Mapping[str, str] | None = None,
) -> str:
    """Build the URL a user is redirected to in order to authorize this client.

    :param authorization_endpoint: The provider's authorization endpoint. Must be
        absolute; a query string already on it is preserved and the parameters below are
        merged into it, which is what providers that pin a path parameter require.
    :param client_id: This client's identifier at the provider.
    :param redirect_uri: Where the provider sends the user back. Sent exactly as given,
        and the token exchange must present the same value (RFC 6749 §4.1.3), so it is
        the registered URI rather than anything reflected from a request.
    :param state: Opaque value tying the callback to this request — generate with
        :func:`~forze_identity.oauth.state.generate_state`, keep it in the session, and
        compare it with :func:`hmac.compare_digest` on the callback before anything else.
    :param scopes: Scopes to request, joined with a space (RFC 6749 §3.3). Omitted
        entirely when empty, which is how a provider applies its default grant.
    :param code_challenge: The PKCE challenge from
        :func:`~forze_identity.oauth.pkce.generate_pkce`. When given, ``S256`` is declared
        as the method — the only method this builds, because ``plain`` offers no
        protection against an intercepted code.
    :param extra_params: Provider-specific parameters (``audience``, ``prompt``,
        ``access_type``, …), merged last.
    :returns: The absolute URL to redirect the user to.
    :raises CoreException: When the endpoint is not an absolute URL, when *state* is
        empty, or when *extra_params* repeats a parameter this function owns.
    """

    parts = urlsplit(authorization_endpoint)

    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise exc.validation(
            f"Authorization endpoint must be an absolute http(s) URL: {authorization_endpoint!r}",
        )

    if not state:
        raise exc.validation(
            "Authorization URL requires a non-empty state — it is what ties the callback "
            "to this request, and an empty one cannot be compared",
        )

    extra = dict(extra_params or {})
    reserved = _RESERVED & extra.keys()

    if reserved:
        raise exc.validation(
            f"extra_params may not set {sorted(reserved)} — those are built from the "
            "arguments above, and a silent override would send a different request than "
            "the caller thinks it built",
        )

    params: dict[str, str] = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    }

    scope = " ".join(scopes)

    if scope:
        params["scope"] = scope

    if code_challenge is not None:
        params["code_challenge"] = code_challenge
        params["code_challenge_method"] = "S256"

    params.update(extra)

    # The endpoint's own query survives: a provider that pins a parameter in the URL it
    # publishes (a tenant id, an API version) keeps it, and this appends rather than
    # replaces.
    # `quote_via=quote` so a space becomes %20 rather than "+". In a query string "+"
    # means space only by form-encoding convention, which RFC 3986 does not define, and a
    # provider reading it literally would receive a scope named "a+b" and grant the wrong
    # thing — a failure that surfaces as a permission error nobody can trace to the URL.
    encoded = urlencode(params, quote_via=quote)
    query = f"{parts.query}&{encoded}" if parts.query else encoded

    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
