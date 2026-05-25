from typing import TypedDict

from forze_fastapi.transport.http.auth import AuthnRequirement
from forze_fastapi.transport.http.policies import Policy

# ----------------------- #


class RouteOpts(TypedDict, total=False):
    """Per-route overrides when attaching generated routes."""

    path_override: str
    authn: AuthnRequirement
    policies: list[Policy]
    include_in_schema: bool
