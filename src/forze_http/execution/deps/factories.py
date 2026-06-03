"""HTTP service dep factories."""

from typing import final

import attrs

from forze.application.contracts.http import HttpServiceDepPort, HttpServiceSpec
from forze.application.contracts.secrets import SecretsDepKey
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from forze_http.adapters.http_service import HttpxHttpServiceAdapter
from forze_http.execution.deps.configs import HttpxHttpServiceConfig
from forze_http.execution.deps.keys import HttpxClientDepKey
from forze_http.kernel.client import HttpxClientPort, RoutedHttpxClient

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableHttpxHttpService(HttpServiceDepPort):
    """Configurable httpx HTTP service adapter."""

    config: HttpxHttpServiceConfig = attrs.field(
        validator=attrs.validators.instance_of(HttpxHttpServiceConfig),
    )

    _service_routed: RoutedHttpxClient | None = attrs.field(init=False, default=None)

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: HttpServiceSpec,
    ) -> HttpxHttpServiceAdapter:
        client = self._resolve_client(ctx, spec)

        return HttpxHttpServiceAdapter(
            client=client,
            config=self.config,
            spec=spec,
        )

    # ....................... #

    def _resolve_client(
        self,
        ctx: ExecutionContext,
        spec: HttpServiceSpec,
    ) -> HttpxClientPort:
        dep_client = ctx.deps.provide(HttpxClientDepKey)

        if not self.config.tenant_aware:
            return dep_client

        if self.config.secret_ref_for_tenant is not None:
            if self._service_routed is None:
                self._service_routed = RoutedHttpxClient(
                    secrets=ctx.deps.provide(SecretsDepKey),
                    secret_ref_for_tenant=self.config.secret_ref_for_tenant,
                    tenant_provider=ctx.inv_ctx.get_tenant,
                    backend=f"http.{spec.name}",
                )

            return self._service_routed

        if isinstance(dep_client, RoutedHttpxClient):
            return dep_client

        raise exc.configuration(
            "tenant_aware HTTP service requires secret_ref_for_tenant on the service "
            "config or a RoutedHttpxClient registered at HttpxClientDepKey",
        )
