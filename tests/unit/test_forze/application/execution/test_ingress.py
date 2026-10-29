"""The shared ingress: exposure, input validation, identity, API-key auth, bind-and-run."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import AliasChoices, AliasPath, BaseModel, ConfigDict, Field, SecretStr

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.execution import Handler
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverDepKey
from forze.application.execution import Deps, OperationDescriptor
from forze.application.execution.context import ExecutionContext
from forze.application.execution.ingress import (
    IngressIdentityResolver,
    StaticIdentityResolver,
    accepted_input_names,
    api_key_credentials,
    authenticate_api_key,
    invoke_at_ingress,
    mint_invocation_metadata,
    refuse_sensitive,
    resolve_principal_tenant,
    select_operations,
    validate_operation_input,
)
from forze.application.execution.ingress.arguments import _alias_names
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #


class _In(BaseModel):
    n: int
    label: str = "x"


class _AliasIn(BaseModel):
    n: int = Field(alias="count")


class _OpenIn(BaseModel):
    model_config = ConfigDict(extra="allow")

    n: int


class _SecretIn(BaseModel):
    token: SecretStr


@attrs.define(slots=True)
class _Echo(Handler[_In, int]):
    async def __call__(self, args: _In) -> int:
        return args.n


def _registry(*, sensitive: bool = False) -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "calc.read": lambda _c: _Echo(),
            "calc.write": lambda _c: _Echo(),  # COMMAND (default)
        }
    )
    reg = reg.set_descriptor("calc.read", OperationDescriptor(input_type=_In, sensitive=sensitive))
    reg = reg.bind("calc.read").as_query().finish()

    return reg.freeze()


# ....................... #


class TestSelectOperations:
    def _select(self, **kw: Any) -> Any:
        opts: dict[str, Any] = {"include_writes": False, "operations": None}
        opts.update(kw)

        return select_operations(_registry().catalog(), surface="test tool", **opts)

    def test_queries_only_by_default(self) -> None:
        assert set(self._select()) == {"calc.read"}

    def test_include_writes_adds_commands(self) -> None:
        assert set(self._select(include_writes=True)) == {"calc.read", "calc.write"}

    def test_an_allowlist_narrows_the_surface(self) -> None:
        assert set(self._select(include_writes=True, operations=["calc.write"])) == {"calc.write"}

    def test_a_single_key_is_one_operation_not_its_characters(self) -> None:
        assert set(self._select(operations="calc.read")) == {"calc.read"}

    def test_an_empty_allowlist_is_refused_naming_the_surface(self) -> None:
        with pytest.raises(CoreException, match="test tool allowlist"):
            self._select(operations=[])

    def test_an_unknown_name_is_refused(self) -> None:
        with pytest.raises(CoreException, match="calc.typo"):
            self._select(operations=["calc.read", "calc.typo"])

    def test_a_command_needs_include_writes(self) -> None:
        with pytest.raises(CoreException, match="include_writes=True: calc.write"):
            self._select(operations=["calc.write"])


class TestSelectStreamingOperations:
    """A byte-stream result has no JSON form, so no generated surface exposes it."""

    def _catalog(self) -> Any:
        reg = OperationRegistry(handlers={"calc.read": lambda _c: _Echo()})
        reg = reg.bind("calc.read").as_query().finish()
        reg = (
            reg.register(
                "calc.stream",
                lambda _c: _Echo(),
                descriptor=OperationDescriptor(input_type=_In, streams=True),
            )
            .as_query()
            .finish()
        )

        return reg.freeze().catalog()

    def test_left_out_by_default(self) -> None:
        exposed = select_operations(
            self._catalog(), include_writes=True, operations=None, surface="t"
        )

        assert set(exposed) == {"calc.read"}

    def test_refused_when_allowlisted_naming_the_surface(self) -> None:
        with pytest.raises(CoreException, match=r"test tool allowlist .*: calc\.stream$") as ei:
            select_operations(
                self._catalog(),
                include_writes=False,
                operations=["calc.read", "calc.stream"],
                surface="test tool",
            )

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestRefuseSensitive:
    def test_a_sensitive_operation_is_refused_naming_the_surface(self) -> None:
        catalog = _registry(sensitive=True).catalog()

        with pytest.raises(CoreException, match="register test tools: operation 'calc.read'"):
            refuse_sensitive(catalog, ["calc.read"], surface="test tool")

    def test_other_operations_pass(self) -> None:
        refuse_sensitive(_registry().catalog(), ["calc.read", "calc.write"], surface="t")


# ....................... #


class TestValidateOperationInput:
    def test_no_input_type_yields_none(self) -> None:
        assert validate_operation_input(None, {"x": 1}, code="c", forbid_unknown=False) is None

    def test_no_input_type_refuses_any_argument_when_forbidding_unknown(self) -> None:
        with pytest.raises(CoreException) as caught:
            validate_operation_input(None, {"x": 1}, code="c", forbid_unknown=True)

        assert caught.value.code == "c"
        assert caught.value.details == {"unknown": ["x"]}

    def test_valid_input_becomes_the_dto(self) -> None:
        args = validate_operation_input(
            OperationDescriptor(input_type=_In), {"n": 2}, code="c", forbid_unknown=True
        )

        assert args == _In(n=2)

    def test_a_rejection_never_echoes_the_value(self) -> None:
        with pytest.raises(CoreException) as caught:
            validate_operation_input(
                OperationDescriptor(input_type=_SecretIn),
                {"token": 123456789},
                code="surface_invalid",
                forbid_unknown=False,
                summary="Invalid things",
            )

        assert caught.value.kind is ExceptionKind.VALIDATION
        assert caught.value.code == "surface_invalid"
        assert caught.value.summary == "Invalid things"
        assert "123456789" not in repr(caught.value.details)

    def test_an_unknown_name_is_dropped_unless_forbidden(self) -> None:
        descriptor = OperationDescriptor(input_type=_In)

        assert validate_operation_input(
            descriptor, {"n": 1, "nn": 2}, code="c", forbid_unknown=False
        ) == _In(n=1)

        with pytest.raises(CoreException) as caught:
            validate_operation_input(descriptor, {"n": 1, "nn": 2}, code="c", forbid_unknown=True)

        assert caught.value.details == {"unknown": ["nn"]}
        assert "'nn'" in caught.value.summary

    def test_an_alias_is_not_unknown(self) -> None:
        args = validate_operation_input(
            OperationDescriptor(input_type=_AliasIn), {"count": 3}, code="c", forbid_unknown=True
        )

        assert args.n == 3

    @pytest.mark.parametrize(
        ("config", "accepted"),
        [
            (ConfigDict(), {"count"}),
            (ConfigDict(validate_by_name=True), {"n", "count"}),
            (ConfigDict(populate_by_name=True), {"n", "count"}),
            (ConfigDict(validate_by_name=True, validate_by_alias=False), {"n"}),
            # With alias reading off, pydantic reads by name without being told to.
            (ConfigDict(validate_by_alias=False), {"n"}),
            # Pydantic's back-compat rule turns alias reading back on here.
            (ConfigDict(populate_by_name=True, validate_by_alias=False), {"n", "count"}),
            (ConfigDict(populate_by_name=True, validate_by_name=False), {"count"}),
        ],
    )
    def test_only_the_names_the_config_reads_are_accepted(
        self, config: ConfigDict, accepted: set[str]
    ) -> None:
        # Pydantic drops a name its config does not read, so accepting it would run the
        # operation on the default rather than the caller's value.
        class _Configured(BaseModel):
            model_config = config
            n: int = Field(default=1, alias="count")

        assert accepted_input_names(_Configured) == accepted

        # Pydantic itself is the oracle: a name is accepted exactly when it is read.
        for name in ("n", "count"):
            assert (name in accepted) == (_Configured.model_validate({name: 5}).n == 5)

        for name in {"n", "count"} - accepted:
            with pytest.raises(CoreException) as caught:
                validate_operation_input(
                    OperationDescriptor(input_type=_Configured),
                    {name: 5},
                    code="c",
                    forbid_unknown=True,
                )

            assert caught.value.details == {"unknown": [name]}

    def test_by_name_reads_an_aliased_field_by_its_own_name(self) -> None:
        args = validate_operation_input(
            OperationDescriptor(input_type=_AliasIn),
            {"n": 3},
            code="c",
            forbid_unknown=True,
            by_name=True,
        )

        assert args.n == 3

    def test_by_name_keeps_an_alias_off_another_fields_name(self) -> None:
        # ``a``'s alias is ``b``'s name: read by alias too, ``a`` would take ``b``'s value.
        class _Clash(BaseModel):
            a: int = Field(default=0, validation_alias="b")
            b: int = 0

        args = validate_operation_input(
            OperationDescriptor(input_type=_Clash),
            {"a": 1, "b": 2},
            code="c",
            forbid_unknown=True,
            by_name=True,
        )

        assert (args.a, args.b) == (1, 2)

    def test_an_open_dto_takes_extension_fields(self) -> None:
        args = validate_operation_input(
            OperationDescriptor(input_type=_OpenIn), {"n": 1, "x": 2}, code="c", forbid_unknown=True
        )

        assert args.n == 1


# ....................... #


class TestTheUnreadableAliasFallback:
    """The branch that only runs when a name cannot be enumerated at all.

    It is the check's safety valve: when the accepted set cannot be built, the check must
    step aside rather than refuse what the DTO would accept.

    Pinned against the helpers rather than through a validation, because pydantic refuses
    an index-first ``AliasPath`` as a field alias — the shapes that reach this branch
    cannot be expressed as a DTO field.
    """

    def test_an_alias_shape_the_helper_cannot_read_yields_no_names(self) -> None:
        class _FutureAlias:
            """Neither a string, nor choices, nor a path — a shape pydantic may add later."""

        assert _alias_names(_FutureAlias()) == set()

    def test_a_path_that_starts_at_an_index_yields_no_names(self) -> None:
        # The payload is a list, so there is no top-level key to accept or refuse.
        assert _alias_names(AliasPath(0, "n")) == set()

    def test_choices_carrying_only_unreadable_paths_yield_no_names(self) -> None:
        assert _alias_names(AliasChoices(AliasPath(0, "n"))) == set()

    def test_a_field_whose_names_cannot_be_enumerated_stops_the_check(self) -> None:
        # set() from the helper must become None at the DTO level — "do not check",
        # not "accepts nothing", which would refuse every argument the DTO does take.
        class _Unreadable:
            validation_alias = AliasPath(0, "n")
            alias = None

        class _Fields:
            model_config: dict[str, object] = {}
            model_fields = {"n": _Unreadable()}

        assert accepted_input_names(_Fields) is None  # type: ignore[arg-type]

    def test_a_readable_dto_is_still_checked(self) -> None:
        # The contrast that makes the step-aside meaningful rather than a blanket skip.
        assert accepted_input_names(_AliasIn) == {"count"}


# ....................... #


class TestMintInvocationMetadata:
    def test_ids_are_fresh(self) -> None:
        first, second = mint_invocation_metadata(), mint_invocation_metadata()

        assert first.execution_id != second.execution_id
        assert first.correlation_id != second.correlation_id
        assert first.causation_id is None

    def test_a_carried_correlation_and_causation_are_kept(self) -> None:
        correlation, causation = uuid4(), uuid4()
        metadata = mint_invocation_metadata(correlation_id=correlation, causation_id=causation)

        assert metadata.correlation_id == correlation
        assert metadata.causation_id == causation


# ....................... #


class TestInvokeAtIngress:
    async def test_binds_the_envelope_for_the_call_only(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        seen: dict[str, Any] = {}

        @attrs.define(slots=True)
        class _Capture(Handler[_In, int]):
            async def __call__(self, args: _In) -> int:
                seen["metadata"] = ctx.inv_ctx.get_metadata()
                seen["authn"] = ctx.inv_ctx.get_authn()
                seen["tenant"] = ctx.inv_ctx.get_tenant()
                seen["key"] = ctx.inv_ctx.get_idempotency_key()
                seen["deadline"] = ctx.inv_ctx.get_deadline()
                return args.n

        reg = OperationRegistry(handlers={"calc.capture": lambda _c: _Capture()}).freeze()
        authn = AuthnIdentity(principal_id=uuid4())
        tenant = TenantIdentity(tenant_id=uuid4())
        metadata = mint_invocation_metadata()

        result = await invoke_at_ingress(
            reg,
            ctx,
            "calc.capture",
            _In(n=7),
            authn=authn,
            tenant=tenant,
            metadata=metadata,
            idempotency_key="k-1",
            deadline=30.0,
        )

        assert result == 7
        assert seen["metadata"] == metadata
        assert seen["authn"] == authn
        assert seen["tenant"] == tenant
        assert seen["key"] == "k-1"
        assert seen["deadline"] is not None
        assert ctx.inv_ctx.get_metadata() is None
        assert ctx.inv_ctx.get_idempotency_key() is None
        assert ctx.inv_ctx.get_deadline() is None

    async def test_no_key_never_inherits_an_outer_one(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        seen: list[str | None] = []

        @attrs.define(slots=True)
        class _Capture(Handler[_In, int]):
            async def __call__(self, args: _In) -> int:
                seen.append(ctx.inv_ctx.get_idempotency_key())
                return args.n

        reg = OperationRegistry(handlers={"calc.capture": lambda _c: _Capture()}).freeze()

        with ctx.inv_ctx.bind_idempotency("outer"):
            await invoke_at_ingress(
                reg,
                ctx,
                "calc.capture",
                _In(n=1),
                authn=None,
                tenant=None,
                metadata=mint_invocation_metadata(),
            )

        assert seen == [None]


# ....................... #


class TestStaticIdentityResolver:
    async def test_binds_nothing_by_default(self) -> None:
        assert await StaticIdentityResolver().resolve() == (None, None)

    async def test_binds_the_configured_pair(self) -> None:
        authn = AuthnIdentity(principal_id=uuid4())
        tenant = TenantIdentity(tenant_id=uuid4())

        assert await StaticIdentityResolver(authn=authn, tenant=tenant).resolve() == (
            authn,
            tenant,
        )

    def test_satisfies_the_protocol(self) -> None:
        assert isinstance(StaticIdentityResolver(), IngressIdentityResolver)


# ....................... #


_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"api_key"}))
_PID = uuid4()


class _ApiKeyPort:
    def __init__(self, *, hint: str | None = None) -> None:
        self.hint = hint
        self.seen: ApiKeyCredentials | None = None

    async def authenticate_with_api_key(self, credentials: ApiKeyCredentials) -> AuthnResult:
        self.seen = credentials

        if credentials.key != "secret":
            raise exc.authentication("unknown api key")

        return AuthnResult(identity=AuthnIdentity(principal_id=_PID), issuer_tenant_hint=self.hint)


class _TenantResolver:
    def __init__(self, *, bound: bool = True) -> None:
        self.bound = bound
        self.requested: UUID | None = None

    async def resolve_from_principal(
        self, principal_id: UUID, *, requested_tenant_id: UUID | None = None
    ) -> TenantIdentity | None:
        assert principal_id == _PID
        self.requested = requested_tenant_id

        if not self.bound or requested_tenant_id is None:
            return None

        return TenantIdentity(tenant_id=requested_tenant_id)


def _result(hint: str | None = None) -> AuthnResult:
    return AuthnResult(identity=AuthnIdentity(principal_id=_PID), issuer_tenant_hint=hint)


def _ctx(*, resolver: _TenantResolver | None = None) -> ExecutionContext:
    deps: dict[Any, Any] = {}

    if resolver is not None:
        deps[TenantResolverDepKey] = lambda _c: resolver

    return context_from_deps(Deps.plain(deps))


class TestApiKeyCredentials:
    def test_a_prefixed_key_splits(self) -> None:
        assert api_key_credentials(" pfx:secret ") == ApiKeyCredentials(key="secret", prefix="pfx")

    def test_a_bare_key_is_the_secret(self) -> None:
        assert api_key_credentials("secret") == ApiKeyCredentials(key="secret")

    def test_a_trailing_separator_is_a_prefix_with_an_empty_secret(self) -> None:
        # As over HTTP: ``pfx:`` is not the bare key ``pfx``.
        assert api_key_credentials("pfx:") == ApiKeyCredentials(key="", prefix="pfx")

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (":", ApiKeyCredentials(key="", prefix="")),
            (":secret", ApiKeyCredentials(key="secret", prefix="")),
            ("a:b:c", ApiKeyCredentials(key="b:c", prefix="a")),
            ("pfx : s", ApiKeyCredentials(key=" s", prefix="pfx ")),
        ],
    )
    def test_the_first_separator_splits_as_over_http(
        self, raw: str, expected: ApiKeyCredentials
    ) -> None:
        assert api_key_credentials(raw) == expected


class TestAuthenticateApiKey:
    async def test_the_split_key_reaches_the_port(self) -> None:
        port = _ApiKeyPort()
        ctx = context_from_deps(Deps.plain({AuthnDepKey: lambda _c, _s: port}))

        result = await authenticate_api_key(ctx, _SPEC, "pfx:secret")

        assert result.identity.principal_id == _PID
        assert port.seen == ApiKeyCredentials(key="secret", prefix="pfx")

    async def test_a_bad_key_raises_authentication(self) -> None:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: lambda _c, _s: _ApiKeyPort()}))

        with pytest.raises(CoreException) as caught:
            await authenticate_api_key(ctx, _SPEC, "wrong")

        assert caught.value.kind is ExceptionKind.AUTHENTICATION

    @pytest.mark.parametrize("raw", ["", "  "])
    async def test_a_blank_key_never_reaches_the_port(self, raw: str) -> None:
        port = _ApiKeyPort()
        ctx = context_from_deps(Deps.plain({AuthnDepKey: lambda _c, _s: port}))

        with pytest.raises(CoreException) as caught:
            await authenticate_api_key(ctx, _SPEC, raw)

        assert caught.value.kind is ExceptionKind.AUTHENTICATION
        assert port.seen is None


class TestResolvePrincipalTenant:
    async def test_nothing_to_resolve_binds_no_tenant(self) -> None:
        assert await resolve_principal_tenant(_ctx(), _result()) is None

    async def test_without_a_resolver_the_credential_tenant_is_trusted(self) -> None:
        tid = uuid4()

        tenant = await resolve_principal_tenant(_ctx(), _result(str(tid)))

        assert tenant == TenantIdentity(tenant_id=tid)

    async def test_the_resolver_validates_the_credential_tenant(self) -> None:
        tid, resolver = uuid4(), _TenantResolver()

        tenant = await resolve_principal_tenant(_ctx(resolver=resolver), _result(str(tid)))

        assert resolver.requested == tid
        assert tenant == TenantIdentity(tenant_id=tid)

    async def test_the_resolver_validates_a_requested_tenant(self) -> None:
        tid, resolver = uuid4(), _TenantResolver()

        await resolve_principal_tenant(_ctx(resolver=resolver), _result(), requested=tid)

        assert resolver.requested == tid

    async def test_a_requested_tenant_conflicting_with_the_credential_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            await resolve_principal_tenant(
                _ctx(resolver=_TenantResolver()), _result(str(uuid4())), requested=uuid4()
            )

        assert caught.value.code == "tenant_conflict"

    async def test_a_requested_tenant_the_resolver_finds_no_binding_for_is_refused(
        self,
    ) -> None:
        resolver = _TenantResolver(bound=False)

        assert await resolve_principal_tenant(_ctx(resolver=resolver), _result()) is None

        with pytest.raises(CoreException) as caught:
            await resolve_principal_tenant(_ctx(resolver=resolver), _result(), requested=uuid4())

        assert caught.value.kind is ExceptionKind.AUTHENTICATION
        assert caught.value.code == "tenant_mismatch"

    async def test_the_credential_tenant_is_kept_when_the_resolver_has_no_binding(
        self,
    ) -> None:
        tid = uuid4()

        tenant = await resolve_principal_tenant(
            _ctx(resolver=_TenantResolver(bound=False)), _result(str(tid))
        )

        assert tenant == TenantIdentity(tenant_id=tid)

    async def test_a_requested_tenant_without_a_resolver_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            await resolve_principal_tenant(_ctx(), _result(), requested=uuid4())

        assert caught.value.kind is ExceptionKind.PRECONDITION
        assert caught.value.code == "tenant_request_unverifiable"

    @pytest.mark.parametrize("resolver", [None, _TenantResolver(bound=False)])
    async def test_a_requested_tenant_matching_the_credential_is_bound(
        self, resolver: _TenantResolver | None
    ) -> None:
        # As over HTTP: the verified credential vouches for its own tenant.
        tid = uuid4()

        tenant = await resolve_principal_tenant(
            _ctx(resolver=resolver), _result(str(tid)), requested=tid
        )

        assert tenant == TenantIdentity(tenant_id=tid)
