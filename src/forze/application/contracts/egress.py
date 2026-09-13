"""Declaring, and consciously accepting, that data leaves the trust boundary.

An integration route that sends business data, personal data or prompts to a third party
is a governance fact about a deployment, not an implementation detail of one adapter. Two
separate statements make it: the **application** declares that a route carries sensitive
data, and the **operator** accepts that it leaves. Collapsing them into one flag would let
"I set the bool" pass for "I decided".

This is a wiring-time gate and an audit marker — not a DLP firewall. Nothing here inspects
a payload or blocks a request; it makes the egress a declared, reviewed wiring fact that
fails closed until someone acknowledges it, so the decision exists in the diff rather than
in somebody's memory.
"""

from typing import Final

from forze.base.exceptions import exc

# ----------------------- #

HTTP_EGRESS_UNACKNOWLEDGED: Final[str] = "http_egress_unacknowledged"
"""Error code for an HTTP route that declares sensitive egress without acknowledging it."""

EGRESS_SENSITIVE_ATTRIBUTE: Final[str] = "forze.egress.sensitive"
"""Span attribute marking a call that carries data outside the trust boundary."""


# ....................... #


def require_egress_acknowledged(
    *,
    subject: str,
    detail: str,
    egress_sensitive: bool,
    acknowledged: bool,
    code: str | None = None,
) -> None:
    """Refuse a config that sends sensitive data out without an operator acknowledgement.

    :param subject: The config class being validated, named in the message.
    :param detail: What leaves and where it goes — the part that tells an operator what
        they are being asked to accept.
    :param egress_sensitive: Whether this route carries data out of the trust boundary.
        A route that does not is unaffected, so an existing deployment sees no change.
    :param acknowledged: The operator's statement that they accept it.
    :param code: Error code for the refusal, when the caller pins one.
    :raises CoreException: ``configuration`` — when sensitive egress is undeclared.
    """

    if not egress_sensitive or acknowledged:
        return

    raise exc.configuration(
        f"{subject} requires acknowledge_data_egress=True: {detail}",
        code=code,
    )
