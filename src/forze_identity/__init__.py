"""Identity plane: authentication, authorization, tenancy — and the specs they bind.

A curated front door. The subpackages stay reachable at their full paths
(``forze_identity.authn``, ``forze_identity.authz``, ``forze_identity.tenancy``); what is
promoted here is the one thing an application cannot discover for itself — the nineteen
document specs the identity plane binds on its behalf. Re-exports resolve lazily (PEP 562),
so importing the package stays cheap.
"""

from typing import TYPE_CHECKING

from forze.base.lazy import lazy_exports

# ----------------------- #

# Curated name -> canonical module (single source of truth for the front door).
_EXPORTS: dict[str, str] = {
    "spec_contributions": "forze_identity.inventory",
    "AUTHN_SPECS": "forze_identity.inventory",
    "AUTHZ_SPECS": "forze_identity.inventory",
    "TENANCY_SPECS": "forze_identity.inventory",
}

__all__ = [
    "AUTHN_SPECS",
    "AUTHZ_SPECS",
    "TENANCY_SPECS",
    "spec_contributions",
]

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS)

if TYPE_CHECKING:
    from forze_identity.inventory import (
        AUTHN_SPECS,
        AUTHZ_SPECS,
        TENANCY_SPECS,
        spec_contributions,
    )
