"""Permanent facade for the core authn orchestrator.

:class:`AuthnOrchestrator` moved to :mod:`forze.application.integrations.authn`: it is
pure port composition over ``forze.application.contracts.authn`` (no crypto, no backend
dependencies), and adapter planes that may not import ``forze_identity`` — notably
``forze_mock`` — need it to run real authn flows. ``forze_identity.authn`` stays the
user-facing import path (identity → core imports are allowed), so this re-export is
permanent, not a deprecation shim.
"""

from forze.application.integrations.authn import AuthnOrchestrator

# ----------------------- #

__all__ = [
    "AuthnOrchestrator",
]
