from typing import Any, final

import attrs

from forze.application.contracts.base import DepKey
from forze.application.contracts.auth import (
    ApiKeyLifecycleDepKey,
    AuthenticationDepKey,
    AuthorizationDepKey,
    TokenLifecycleDepKey,
)
from forze.application.execution import Deps

from .deps import (
    ConfigurableDocumentApiKeyLifecycle,
    ConfigurableDocumentAuthentication,
    ConfigurableDocumentAuthorization,
    ConfigurableDocumentTokenLifecycle,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentAuthDepsModule:
    """Dependency module registering document-backed auth adapters."""

    route: str | None = attrs.field(default=None)
    """Optional auth route. When omitted, adapters are registered as plain deps."""

    # ....................... #

    def __call__(self) -> Deps[str]:
        deps: dict[DepKey[Any], Any] = {
            AuthenticationDepKey: ConfigurableDocumentAuthentication(),
            AuthorizationDepKey: ConfigurableDocumentAuthorization(),
            TokenLifecycleDepKey: ConfigurableDocumentTokenLifecycle(),
            ApiKeyLifecycleDepKey: ConfigurableDocumentApiKeyLifecycle(),
        }

        if self.route is None:
            return Deps.plain(deps)

        return Deps.routed_group(deps, routes=frozenset({self.route}))
