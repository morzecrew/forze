"""Credential swapping on a MongoDB connection string.

``pymongo.uri_parser.parse_uri`` decomposes a URI but has no inverse, and rebuilding one
from its parts loses whatever it normalised away — most visibly ``mongodb+srv://``, whose
single hostname is resolved into a node list that cannot be turned back into the original.

So this rewrites the *userinfo* in place and leaves every other byte of the string alone.
The credential is the only thing a rotation changes; the host list, database, replica-set
name and option string are facts about the deployment that must survive it verbatim.
"""

from urllib.parse import quote_plus, unquote_plus

from forze.base.exceptions import exc

__all__ = ["mongo_uri_password", "mongo_uri_username", "with_mongo_credentials"]

# ....................... #

_SCHEMES = ("mongodb+srv://", "mongodb://")


def _split(uri: str) -> tuple[str, str, str]:
    """Return ``(scheme, userinfo, remainder)`` for *uri*; userinfo may be empty."""

    for scheme in _SCHEMES:
        if uri.startswith(scheme):
            rest = uri[len(scheme) :]
            # Only the authority carries userinfo, and it ends at the first '/'. A '@' in
            # the path or query (an option value, say) must not be mistaken for one.
            authority = rest.split("/", 1)[0]

            if "@" not in authority:
                return scheme, "", rest

            userinfo, _, after = rest.partition("@")

            return scheme, userinfo, after

    raise exc.configuration(
        "Secret under rotation is not a MongoDB connection string "
        "(expected a mongodb:// or mongodb+srv:// URI).",
    )


# ....................... #


def mongo_uri_username(uri: str) -> str:
    """The username a URI authenticates as, percent-decoded."""

    _, userinfo, _ = _split(uri)
    username = unquote_plus(userinfo.partition(":")[0])

    if not username:
        raise exc.configuration("MongoDB URI under rotation names no user.")

    return username


# ....................... #


def mongo_uri_password(uri: str) -> str:
    """The password a URI authenticates with, percent-decoded (empty when it carries none)."""

    _, userinfo, _ = _split(uri)

    return unquote_plus(userinfo.partition(":")[2])


# ....................... #


def with_mongo_credentials(uri: str, *, username: str, password: str) -> str:
    """Return *uri* with its userinfo replaced.

    Both parts are percent-encoded: a password is minted from raw entropy and will contain
    ``/``, ``:`` and ``@`` often enough that leaving it unescaped would silently produce a
    URI pointing somewhere else.
    """

    scheme, _, remainder = _split(uri)

    return f"{scheme}{quote_plus(username)}:{quote_plus(password)}@{remainder}"
