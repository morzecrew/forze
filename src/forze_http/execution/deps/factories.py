"""HTTP service dep factories."""

from typing import final

import attrs

from forze.application.contracts.http import (
    HttpServiceDepKey,
    HttpServiceDepPort,
    HttpServiceSpec,
)
from forze.application.contracts.secrets import SecretsDepKey
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze_http.adapters.http_service import HttpServiceAdapter
from forze_http.adapters.http_transport import HttpServiceTransport
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.execution.deps.keys import HttpClientDepKey
from forze_http.kernel.client import HttpClientPort, RoutedHttpClient

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class ConfigurableHttpService(HttpServiceDepPort):
    """Configurable httpx HTTP service adapter."""

    config: HttpServiceConfig = attrs.field(
        validator=attrs.validators.instance_of(HttpServiceConfig),
    )

    _service_routed: RoutedHttpClient | None = attrs.field(init=False, default=None)

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: HttpServiceSpec,
    ) -> HttpServiceAdapter:
        client = self._resolve_client(ctx, spec)

        return HttpServiceAdapter(
            client=client,
            config=self.config,
            spec=spec,
        )

    # ....................... #

    def _resolve_client(
        self,
        ctx: ExecutionContext,
        spec: HttpServiceSpec,
    ) -> HttpClientPort:
        dep_client = ctx.deps.provide(HttpClientDepKey)

        if not self.config.tenant_aware:
            return dep_client

        if self.config.secret_ref_for_tenant is not None:
            if self._service_routed is None:
                self._service_routed = RoutedHttpClient(
                    secrets=ctx.deps.provide(SecretsDepKey),
                    secret_ref_for_tenant=self.config.secret_ref_for_tenant,
                    tenant_provider=ctx.inv_ctx.get_tenant,
                    backend=f"http.{spec.name}",
                    # A tenant-routed service learns its URL from each tenant's secret, so
                    # the config's own cleartext check never sees one. The declaration
                    # travels with the client instead, and the check runs per tenant.
                    egress_sensitive=self.config.egress_sensitive,
                )

            return self._service_routed

        if isinstance(dep_client, RoutedHttpClient):
            return dep_client

        raise exc.configuration(
            "tenant_aware HTTP service requires secret_ref_for_tenant on the service "
            "config or a RoutedHttpClient registered at HttpClientDepKey",
        )


# ....................... #


def http_service_transport(ctx: ExecutionContext, spec: HttpServiceSpec) -> HttpServiceTransport:
    """An httpx transport for a vendor SDK, over the declared HTTP service *spec*.

    Resolved through the service's dep key, the way ``ctx.http.service(spec)`` is: the same
    config, so the same egress acknowledgement already passed at wiring, and the same client,
    routed per tenant when the service is. Pass it as ``httpx.AsyncClient(transport=...)``.

    :param ctx: The execution context the service is wired in.
    :param spec: The declared service the SDK's requests go through.
    :returns: A transport holding every request to the service's base URL or tenant route.
    :raises CoreException: ``configuration`` when *spec* is not wired as a ``forze_http``
        service — undeclared, outside the spec inventory, or served by another backend.
    """

    if ctx.deps.inventory_guard is not None:
        ctx.deps.inventory_guard(HttpServiceDepKey.name, str(spec.name))

    factory = ctx.deps.provide(HttpServiceDepKey, route=spec.name)

    if not isinstance(factory, ConfigurableHttpService):
        raise exc.configuration(
            f"HTTP service {str(spec.name)!r} is not wired through forze_http, so there is "
            "no declared route to send an SDK's requests through",
        )

    adapter = factory(ctx, spec)

    return HttpServiceTransport(client=adapter.client, config=adapter.config, spec=spec)
