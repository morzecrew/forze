"""Address predicates shared by the settings model and the client.

Package-private, and here rather than in either of them because both need it and neither
should depend on the other: the settings model decides whether a plaintext address is
allowed, and the client has to make the assumption behind that decision true.
"""

from ipaddress import ip_address

# ----------------------- #


def is_loopback(hostname: str | None) -> bool:
    """Whether *hostname* names this machine, by name or by address.

    :param hostname: Host part of a URL, brackets included or not.
    :returns: ``True`` for ``localhost`` and any loopback address.
    """

    if not hostname:
        return False

    if hostname == "localhost":
        return True

    try:
        return ip_address(hostname.strip("[]")).is_loopback
    except ValueError:
        return False


# ....................... #

__all__ = ["is_loopback"]
