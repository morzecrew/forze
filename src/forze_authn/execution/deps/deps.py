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
from forze.base.errors import CoreError

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
from .configs import AuthnRouteCaps, AuthnSharedServices

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableAuthn:
    """Build :class:`~forze_authn.adapters.authentication.AuthnAdapter` from shared services + caps."""

    shared: AuthnSharedServices
    caps: AuthnRouteCaps

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> AuthnPort:
        _ = spec

        pa_qry = ctx.doc_query(password_account_spec) if self.caps.password else None

        ak_qry = ctx.doc_query(api_key_account_spec) if self.caps.api_key else None

        return AuthnAdapter(
            access_svc=self.shared.access_svc if self.caps.bearer else None,
            password_svc=self.shared.password_svc if self.caps.password else None,
            api_key_svc=self.shared.api_key_svc if self.caps.api_key else None,
            pa_qry=pa_qry,
            ak_qry=ak_qry,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTokenLifecycle:
    """Build :class:`~forze_authn.adapters.token_lifecycle.TokenLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> TokenLifecyclePort:
        _ = spec

        if self.shared.access_svc is None or self.shared.refresh_svc is None:
            raise CoreError(
                "Token lifecycle requires kernel.access_token_secret and kernel.refresh_token_pepper",
            )

        return TokenLifecycleAdapter(
            access_svc=self.shared.access_svc,
            refresh_svc=self.shared.refresh_svc,
            session_qry=ctx.doc_query(session_spec),
            session_cmd=ctx.doc_command(session_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordLifecycle:
    """Build :class:`~forze_authn.adapters.password_lifecycle.PasswordLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordLifecyclePort:
        _ = spec

        if self.shared.password_svc is None:
            raise CoreError("Password lifecycle requires kernel.password")

        return PasswordLifecycleAdapter(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc_query(password_account_spec),
            pa_cmd=ctx.doc_command(password_account_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableApiKeyLifecycle:
    """Build :class:`~forze_authn.adapters.api_key_lifecycle.ApiKeyLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> ApiKeyLifecyclePort:
        _ = spec

        if self.shared.api_key_svc is None:
            raise CoreError("API key lifecycle requires kernel.api_key_pepper")

        return ApiKeyLifecycleAdapter(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc_query(api_key_account_spec),
            ak_cmd=ctx.doc_command(api_key_account_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordAccountProvisioning:
    """Build :class:`~forze_authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordAccountProvisioningPort:
        _ = spec

        if self.shared.password_svc is None:
            raise CoreError("Password provisioning requires kernel.password")

        return PasswordAccountProvisioningAdapter(
            password_svc=self.shared.password_svc,
            password_account_qry=ctx.doc_query(password_account_spec),
            password_account_cmd=ctx.doc_command(password_account_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )
