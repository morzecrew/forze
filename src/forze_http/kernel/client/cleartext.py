"""One predicate for "this destination would carry something in clear".

Shared because the question is asked in two places that cannot see each other: a static
service knows its base URL at wiring, and a tenant-routed one only learns the tenant's URL
when it resolves that tenant's credentials. A second copy of the rule is how the two
answers start disagreeing — and the tenant path is the one where a mistake affects one
tenant quietly rather than the whole deployment loudly.
"""

from ipaddress import ip_address
from urllib.parse import urlsplit

# ----------------------- #


def is_loopback_host(host: str) -> bool:
    """Whether *host* names the local machine — a developer's own setup.

    Parsed rather than matched against a list of spellings: ``127.0.0.2`` is as loopback as
    ``127.0.0.1``, and a check that fires on one and not the other reads as a bug. A
    non-address host is loopback only when it is literally ``localhost``; what a name
    resolves to is not something a config can know.
    """

    if host == "localhost":
        return True

    try:
        return ip_address(host).is_loopback

    except ValueError:
        return False


# ....................... #


def is_cleartext_destination(url: str | None) -> bool:
    """Whether sending to *url* puts bytes on the wire in clear.

    ``None`` and anything that is not ``http://`` are false: an ``https`` destination is
    fine, and a URL this cannot parse is not this function's business to refuse. Loopback
    is exempt — a warning that fires on a developer's own machine is a warning they learn
    to ignore.
    """

    if url is None:
        return False

    try:
        parts = urlsplit(url)

    except ValueError:
        return False

    if parts.scheme != "http":
        return False

    return not is_loopback_host(parts.hostname or "")
