"""Lifecycle and provisioning adapters.

The pre-refactor :class:`AuthnAdapter` was decomposed into separate verifier
implementations under :mod:`forze_authn.verifiers` and an orchestrator under
:mod:`forze_authn.orchestrator`.
"""

from .api_key_lifecycle import ApiKeyLifecycleAdapter
from .password_lifecycle import PasswordLifecycleAdapter
from .password_provisioning import PasswordAccountProvisioningAdapter
from .password_reset import PasswordResetAdapter
from .principal_deactivation import PrincipalDeactivationAdapter
from .principal_eligibility import PolicyPrincipalEligibilityAdapter
from .token_lifecycle import TokenLifecycleAdapter

# ----------------------- #

__all__ = [
    "ApiKeyLifecycleAdapter",
    "PasswordAccountProvisioningAdapter",
    "PasswordLifecycleAdapter",
    "PasswordResetAdapter",
    "PolicyPrincipalEligibilityAdapter",
    "PrincipalDeactivationAdapter",
    "TokenLifecycleAdapter",
]
