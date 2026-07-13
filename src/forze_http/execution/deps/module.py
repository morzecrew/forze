"""HTTP dependency module for the application kernel."""

from typing import Any, final

import attrs

from forze.application.contracts.deps import Deps, DepsModule, merge_deps, routed_from_mapping
from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.tenancy import warn_integration_routes
from forze.base.primitives import MappingConverter, StrKeyMapping
from forze_http.execution._logger import logger
from forze_http.execution.deps._warnings import HTTP_SERVICE_WARNING
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.execution.deps.factories import ConfigurableHttpService
from forze_http.execution.deps.keys import HttpClientDepKey
from forze_http.kernel.client import HttpClientPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HttpDepsModule(DepsModule):
    """Registers httpx client and HTTP service ports."""

    client: HttpClientPort
    """Pre-constructed httpx client (single base URL or routed)."""

    services: StrKeyMapping[HttpServiceConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
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
        plain: dict[Any, Any] = {HttpClientDepKey: self.client}
        plain_deps = Deps.plain(plain)
        service_deps = Deps()

        if self.services:
            service_deps = routed_from_mapping(
                self.services,
                bindings=[(HttpServiceDepKey, ConfigurableHttpService)],
            )

        return merge_deps(plain_deps, service_deps)
