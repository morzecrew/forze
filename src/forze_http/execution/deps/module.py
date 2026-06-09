"""HTTP dependency module for the application kernel."""

from typing import Any, Mapping, final

import attrs

from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.tenancy import warn_integration_routes
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import StrKey
from forze_http.execution.deps._warnings import HTTP_SERVICE_WARNING
from forze_http.execution.deps.configs import HttpxHttpServiceConfig
from forze_http.execution.deps.factories import ConfigurableHttpxHttpService
from forze_http.execution.deps.keys import HttpxClientDepKey
from forze_http.execution._logger import logger
from forze_http.kernel.client import HttpxClientPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpxDepsModule(DepsModule):
    """Registers httpx client and HTTP service ports."""

    client: HttpxClientPort
    """Pre-constructed httpx client (single base URL or routed)."""

    services: Mapping[StrKey, HttpxHttpServiceConfig] | None = attrs.field(
        default=None,
    )
    """Mapping from :class:`~forze.application.contracts.http.HttpServiceSpec` name to config."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="HTTP",
            routes=self.services,
            warning=HTTP_SERVICE_WARNING,
            log_warning=logger.warning,
        )

    # ....................... #

    def __call__(self) -> Deps:
        plain: dict[Any, Any] = {HttpxClientDepKey: self.client}
        plain_deps = Deps.plain(plain)
        service_deps = Deps()

        if self.services:
            service_deps = routed_from_mapping(
                self.services,
                bindings=[(HttpServiceDepKey, ConfigurableHttpxHttpService)],
            )

        return merge_deps(plain_deps, service_deps)
