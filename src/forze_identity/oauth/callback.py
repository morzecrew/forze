"""Reading an authorization callback, in the order that makes it safe.

The callback is where OAuth clients get compromised, and the failure is almost never a
missing check — it is checks in the wrong order, or a check that ran on something the
attacker chose. So this is one function rather than three: a caller cannot compare the
state after exchanging a code it should not have accepted, because it cannot reach the
steps separately.

Pure and direction-neutral, beside the ``state`` it verifies and the PKCE pair it does
not touch: what the code is *for* — a login or an outbound grant — changes nothing about
how the callback is read.
"""

from collections.abc import Mapping
from hmac import compare_digest

from forze.base.exceptions import exc

# ----------------------- #

CALLBACK_STATE_MISMATCH_CODE = "oauth_callback_state_mismatch"
"""The returned ``state`` did not match the session's — a CSRF'd or replayed callback."""

CALLBACK_PROVIDER_ERROR_CODE = "oauth_callback_provider_error"
"""The provider reported a failure instead of issuing a code (RFC 6749 §4.1.2.1)."""

CALLBACK_NO_CODE_CODE = "oauth_callback_missing_code"
"""Neither a code nor an error — a callback that is neither success nor failure."""


# ....................... #


def read_authorization_callback(
    params: Mapping[str, str],
    *,
    expected_state: str | None,
) -> str:
    """Validate a callback's parameters and return the authorization code.

    Three checks, in this order, and the order is the contract:

    1. **State, first and constant-time.** Compared with
       :func:`hmac.compare_digest` before anything else is read, so a callback that did
       not come from a request this session started is rejected before its contents matter.
       An absent *expected_state* is a rejection too: a session with no stored state is
       either a replay of a consumed callback or a request nobody started, and both look
       identical from here.
    2. **A provider error is a failure.** ``error=access_denied`` and its siblings are
       surfaced, never mistaken for a code. A user who declined consent must not produce a
       half-connected account.
    3. **A code must actually be present.**

    :param params: The callback's query parameters.
    :param expected_state: The ``state`` this session stored before redirecting — from the
       session, never from the request.
    :returns: The single-use authorization code.
    :raises CoreException: :data:`CALLBACK_STATE_MISMATCH_CODE`,
        :data:`CALLBACK_PROVIDER_ERROR_CODE`, or :data:`CALLBACK_NO_CODE_CODE`.
    """

    returned = params.get("state") or ""

    if not expected_state or not compare_digest(returned, expected_state):
        # Deliberately says nothing about which side was missing or how they differed: the
        # caller cannot act on the difference, and an attacker should not learn from it.
        raise exc.authentication(
            "Authorization callback state does not match the one this session started with",
            code=CALLBACK_STATE_MISMATCH_CODE,
        )

    error = params.get("error")

    if error:
        raise exc.precondition(
            f"Authorization was not granted: {error}",
            code=CALLBACK_PROVIDER_ERROR_CODE,
            # The provider's own code, which a connect UI needs to tell "the user said no"
            # from "this client is misconfigured". The description is left out: it is
            # provider-authored text, and an app that wants it can read the raw params.
            details={"error": error},
        )

    code = params.get("code")

    if not code:
        raise exc.validation(
            "Authorization callback carried neither a code nor an error",
            code=CALLBACK_NO_CODE_CODE,
        )

    return code
