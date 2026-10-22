"""The two stacks the authn kit runs on in unit tests.

The identity plane's real adapters on mock documents, and the mock token lifecycle. Both run
on the mock's journal transaction manager, which rolls documents and identity state back with
a failed transaction, as a database would.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from forze.application.contracts.authz import PrincipalRegistryDepKey
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.testing import context_from_deps, context_from_modules
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    password_reset_spec,
    session_spec,
)
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from forze_identity.authz.execution.deps.deps import ConfigurablePrincipalRegistry
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.execution.factories import ConfigurableMockDocument, mock_journal_txmanager

# ----------------------- #

AUTHN_ROUTE = "main"
"""The authn route both stacks wire."""

AUTHN_KERNEL = AuthnKernelConfig(
    access_token_secret=b"k" * 32,
    refresh_token_pepper=b"p" * 32,
    api_key_pepper=b"a" * 32,
    reset_token_pepper=b"r" * 32,
    password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
)
"""The identity stack's kernel: HS256 access tokens signed with ``b"k" * 32``."""


def identity_authn_context(*, tx: bool = True) -> ExecutionContext:
    """The identity plane's adapters over mock documents and the journal transaction manager."""

    mock = MockDepsModule()
    document = ConfigurableMockDocument(module=mock)
    names = {
        spec.name
        for spec in (
            session_spec,
            policy_principal_spec,
            api_key_account_spec,
            password_account_spec,
            password_reset_spec,
        )
    }
    deps = (
        AuthnDepsModule(
            kernel=AUTHN_KERNEL,
            authn={AUTHN_ROUTE: frozenset({"token", "password"})},
            authz_route=AUTHN_ROUTE,
            principal_deactivation={AUTHN_ROUTE},
            token_lifecycle={AUTHN_ROUTE},
            password_lifecycle={AUTHN_ROUTE},
            password_account_provisioning={AUTHN_ROUTE},
            password_reset={AUTHN_ROUTE},
        )()
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: dict.fromkeys(names, document),
                    DocumentCommandDepKey: dict.fromkeys(names, document),
                    PrincipalRegistryDepKey: {AUTHN_ROUTE: ConfigurablePrincipalRegistry()},
                }
            )
        )
        .merge(Deps.plain({MockStateDepKey: mock.state}))
    )

    if tx:
        deps = deps.merge(Deps.plain({TransactionManagerDepKey: mock_journal_txmanager}))

    return context_from_deps(deps)


def mock_authn_context() -> ExecutionContext:
    """The mock token lifecycle."""

    return context_from_modules(MockDepsModule())


async def stack_principal(ctx: ExecutionContext) -> UUID:
    """A principal the stack admits: a policy principal on the identity stack, any id on the mock."""

    if ctx.deps.exists(PrincipalRegistryDepKey, route=AUTHN_ROUTE):
        created = await ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="user")
        )
        return created.id

    return uuid4()
