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
    "MIN_SECRET_BYTES": "forze_identity.authn.services.constants",  # nosec B105
    "spec_contributions": "forze_identity.inventory",
    "AUTHN_SPECS": "forze_identity.inventory",
    "AUTHZ_SPECS": "forze_identity.inventory",
    "TENANCY_SPECS": "forze_identity.inventory",
    "GRANT_RESOLUTION_SPECS": "forze_identity.inventory",
    "AUTHZ_DECISION_SPECS": "forze_identity.inventory",
    "DELEGATION_SPECS": "forze_identity.inventory",
    "PASSWORD_LIFECYCLE_SPECS": "forze_identity.inventory",
    "TENANT_RESOLUTION_SPECS": "forze_identity.inventory",
    "identity_document_names": "forze_identity.inventory",
}

__all__ = [
    "MIN_SECRET_BYTES",
    "AUTHN_SPECS",
    "AUTHZ_DECISION_SPECS",
    "AUTHZ_SPECS",
    "DELEGATION_SPECS",
    "GRANT_RESOLUTION_SPECS",
    "PASSWORD_LIFECYCLE_SPECS",
    "TENANCY_SPECS",
    "TENANT_RESOLUTION_SPECS",
    "identity_document_names",
    "spec_contributions",
]

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS)

if TYPE_CHECKING:
    from forze_identity.authn.services.constants import MIN_SECRET_BYTES
    from forze_identity.inventory import (
        AUTHN_SPECS,
        AUTHZ_DECISION_SPECS,
        AUTHZ_SPECS,
        DELEGATION_SPECS,
        GRANT_RESOLUTION_SPECS,
        PASSWORD_LIFECYCLE_SPECS,
        TENANCY_SPECS,
        TENANT_RESOLUTION_SPECS,
        identity_document_names,
        spec_contributions,
    )
