"""The declaration an application exposes to ``forze ops``, and the identity it runs as.

Imports attrs and core only, so a module that declares an :class:`OpsApp` stays importable
without the ``cli`` extra: the web process can import the same module.
"""

from collections.abc import Callable, Iterable

import attrs

from forze.application.contracts.authn import AuthnIdentity, AuthnSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.application.execution.ingress import refuse_sensitive, select_operations
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyMapping

# ----------------------- #

SURFACE = "ops command"
"""What ``forze ops`` exposes operations as, named in an exposure refusal."""

# ....................... #


@attrs.frozen
class ApiKeyIdentity:
    """Run each call as the principal a verified API key authenticates.

    The key goes through the same authn port HTTP and MCP use, so eligibility, expiry,
    revocation and delegation behave identically. The secret is read from the environment
    variable *env*, or from ``--api-key-file``; never from argv, which ``ps`` and shell
    history show.

    :param authn_spec: Selects the authn route the key verifies against.
    :param env: The environment variable holding the key.
    :param tenancy_route: The tenancy route the principal's tenant resolves on; defaults to
        the authn spec's name, as over MCP.
    """

    authn_spec: AuthnSpec
    env: str = "FORZE_API_KEY"
    tenancy_route: str | None = None


# ....................... #


@attrs.frozen
class AnonymousIdentity:
    """Bind no identity: for operations with no authn requirement."""


# ....................... #


@attrs.frozen(kw_only=True)
class StaticIdentity:
    """Run each call as a fixed identity that nothing verifies.

    For single-tenant internal tools and break-glass inside the cluster, where shell access
    already equals database access. It gives up eligibility, expiry and revocation; every
    operator shares one audit actor and one principal-scoped idempotency namespace.

    :param acknowledge_unverified_identity: Must be ``True``; anything else raises
        ``configuration``, so the choice is never made by accident.
    """

    authn: AuthnIdentity | None = None
    tenant: TenantIdentity | None = None
    acknowledge_unverified_identity: bool = False

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.acknowledge_unverified_identity is not True:
            raise exc.configuration(
                "StaticIdentity binds an identity nothing verifies: pass "
                "acknowledge_unverified_identity=True to accept that",
                code="cli_unverified_identity_unacknowledged",
            )


# ....................... #

type OpsIdentity = ApiKeyIdentity | AnonymousIdentity | StaticIdentity
"""The identity an :class:`OpsApp` runs its calls as; there is no default."""

# ....................... #


def _allowlist(value: Iterable[StrKey] | None) -> tuple[StrKey, ...] | None:
    if value is None:
        return None

    # A single key is one operation, not a sequence of characters.
    return (value,) if isinstance(value, str) else tuple(value)


# ....................... #


@attrs.frozen(kw_only=True)
class OpsApp:
    """What ``forze ops`` lists, describes and calls.

    Exposure follows MCP: queries by default, commands only with *include_writes*, an
    optional allowlist checked like a grant, sensitive read models refused, and operations
    whose result is a byte stream left out (refused when allowlisted). It is checked here,
    so a bad declaration fails while its module imports.

    :param registry: The frozen operation registry.
    :param runtime: Builds the runtime a ``call`` runs on, once per call; ``list`` and
        ``describe`` never call it. It must return a ``SERVERLESS``-profiled runtime.
    :param identity: Who each call runs as.
    :param include_writes: Expose command operations as well as queries.
    :param operations: Expose only these operations, by key.
    :raises CoreException: ``configuration`` for an empty allowlist, an unknown key, a
        command without *include_writes*, a streaming operation in the allowlist, or a
        sensitive operation.
    """

    registry: FrozenOperationRegistry
    runtime: Callable[[], ExecutionRuntime]
    identity: OpsIdentity = attrs.field(
        validator=attrs.validators.instance_of((ApiKeyIdentity, AnonymousIdentity, StaticIdentity))
    )
    include_writes: bool = False
    operations: tuple[StrKey, ...] | None = attrs.field(default=None, converter=_allowlist)
    exposed: StrKeyMapping[StrKey] = attrs.field(
        init=False, default=attrs.Factory(lambda self: _exposure(self), takes_self=True)
    )
    """Exposed command name → operation key."""


# ....................... #


def _exposure(ops: OpsApp) -> StrKeyMapping[StrKey]:
    catalog = ops.registry.catalog()
    exposed = select_operations(
        catalog,
        include_writes=ops.include_writes,
        operations=ops.operations,
        surface=SURFACE,
    )
    refuse_sensitive(catalog, exposed.values(), surface=SURFACE)

    return exposed
