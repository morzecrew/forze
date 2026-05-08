"""Configurable authn dependency factories resolving document ports from execution context."""

from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecyclePort,
    AuthnPort,
    AuthnSpec,
    PasswordAccountProvisioningPort,
    PasswordLifecyclePort,
    TokenLifecyclePort,
)
from forze.application.execution import ExecutionContext

from ...adapters import (
    ApiKeyLifecycleAdapter,
    AuthnAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    TokenLifecycleAdapter,
)
from ...application.specs import (
    api_key_account_spec,
    password_account_spec,
    principal_spec,
    session_spec,
)
from .configs import (
    ApiKeyLifecycleRouteConfig,
    AuthnRouteConfig,
    PasswordLifecycleRouteConfig,
    PasswordProvisioningRouteConfig,
    TokenLifecycleRouteConfig,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableAuthn:
    """Build :class:`~forze_authnz.authn.adapters.authentication.AuthnAdapter` from context + route config."""

    config: AuthnRouteConfig

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> AuthnPort:
        _ = spec

        pa_qry = (
            ctx.doc_query(password_account_spec)
            if self.config.password_svc is not None
            else None
        )

        ak_qry = (
            ctx.doc_query(api_key_account_spec)
            if self.config.api_key_svc is not None
            else None
        )

        return AuthnAdapter(
            access_svc=self.config.access_svc,
            password_svc=self.config.password_svc,
            api_key_svc=self.config.api_key_svc,
            pa_qry=pa_qry,
            ak_qry=ak_qry,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTokenLifecycle:
    """Build :class:`~forze_authnz.authn.adapters.token_lifecycle.TokenLifecycleAdapter`."""

    config: TokenLifecycleRouteConfig

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> TokenLifecyclePort:
        _ = spec

        return TokenLifecycleAdapter(
            access_svc=self.config.access_svc,
            refresh_svc=self.config.refresh_svc,
            session_qry=ctx.doc_query(session_spec),
            session_cmd=ctx.doc_command(session_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordLifecycle:
    """Build :class:`~forze_authnz.authn.adapters.password_lifecycle.PasswordLifecycleAdapter`."""

    config: PasswordLifecycleRouteConfig

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordLifecyclePort:
        _ = spec

        return PasswordLifecycleAdapter(
            password_svc=self.config.password_svc,
            pa_qry=ctx.doc_query(password_account_spec),
            pa_cmd=ctx.doc_command(password_account_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableApiKeyLifecycle:
    """Build :class:`~forze_authnz.authn.adapters.api_key_lifecycle.ApiKeyLifecycleAdapter`."""

    config: ApiKeyLifecycleRouteConfig

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> ApiKeyLifecyclePort:
        _ = spec

        return ApiKeyLifecycleAdapter(
            api_key_svc=self.config.api_key_svc,
            ak_qry=ctx.doc_query(api_key_account_spec),
            ak_cmd=ctx.doc_command(api_key_account_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordAccountProvisioning:
    """Build :class:`~forze_authnz.authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`."""

    config: PasswordProvisioningRouteConfig

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordAccountProvisioningPort:
        _ = spec

        return PasswordAccountProvisioningAdapter(
            password_svc=self.config.password_svc,
            password_account_qry=ctx.doc_query(password_account_spec),
            password_account_cmd=ctx.doc_command(password_account_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )
