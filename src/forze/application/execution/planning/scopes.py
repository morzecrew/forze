from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Self, final, override

import attrs

from forze.application.contracts.execution import (
    Before,
    BeforeStep,
    DispatchStep,
    ExecutionGraph,
    ExecutionPipeline,
    Finally,
    FinallyStep,
    Middleware,
    MiddlewareStep,
    OnFailure,
    OnFailureStep,
    OnSuccess,
    OnSuccessStep,
)
from forze.base.errors import CoreError
from forze.base.primitives import AbstractSequence, StrKey

from .builders import graph_from_sequence, pipe_from_sequence
from .resolvers import resolve_graph, resolve_pipe

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class Scope:
    """Scope plan for a distinct operation."""

    before: AbstractSequence[BeforeStep] = attrs.field(factory=AbstractSequence)
    """Before steps for this scope."""

    wrap: AbstractSequence[MiddlewareStep] = attrs.field(factory=AbstractSequence)
    """Wrap steps for this scope."""

    finally_: AbstractSequence[FinallyStep] = attrs.field(factory=AbstractSequence)
    """Finally steps for this scope."""

    on_failure: AbstractSequence[OnFailureStep] = attrs.field(factory=AbstractSequence)
    """On failure steps for this scope."""

    on_success: AbstractSequence[OnSuccessStep] = attrs.field(factory=AbstractSequence)
    """On success steps for this scope."""

    dispatch: AbstractSequence[DispatchStep] = attrs.field(factory=AbstractSequence)
    """Dispatch steps for this scope."""

    # ....................... #

    @classmethod
    def merge(cls, *scopes: Self) -> Self:
        """Merge multiple scope plans into a single."""

        merged_before = AbstractSequence.merge(
            *[scope.before for scope in scopes],
        )
        merged_wrap = AbstractSequence.merge(
            *[scope.wrap for scope in scopes],
        )
        merged_finally = AbstractSequence.merge(
            *[scope.finally_ for scope in scopes],
        )
        merged_on_failure = AbstractSequence.merge(
            *[scope.on_failure for scope in scopes],
        )
        merged_on_success = AbstractSequence.merge(
            *[scope.on_success for scope in scopes],
        )
        merged_dispatch = AbstractSequence.merge(
            *[scope.dispatch for scope in scopes],
        )

        return cls(
            before=merged_before,
            wrap=merged_wrap,
            finally_=merged_finally,
            on_failure=merged_on_failure,
            on_success=merged_on_success,
            dispatch=merged_dispatch,
        )

    # ....................... #

    def freeze(self) -> FrozenScope:
        frozen_before = graph_from_sequence(self.before)
        frozen_wrap = pipe_from_sequence(self.wrap)
        frozen_finally = pipe_from_sequence(self.finally_)
        frozen_on_failure = pipe_from_sequence(self.on_failure)
        frozen_on_success = graph_from_sequence(self.on_success)
        frozen_dispatch = pipe_from_sequence(self.dispatch)

        return FrozenScope(
            before=frozen_before,
            wrap=frozen_wrap,
            finally_=frozen_finally,
            on_failure=frozen_on_failure,
            on_success=frozen_on_success,
            dispatch=frozen_dispatch,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class TransactionScope(Scope):
    """Transaction scope plan for a distinct operation."""

    route: StrKey | None = None
    """Transaction route for this scope."""

    after_commit: AbstractSequence[OnSuccessStep] = attrs.field(
        factory=AbstractSequence
    )
    """After commit steps for this scope."""

    dispatch_after_commit: AbstractSequence[DispatchStep] = attrs.field(
        factory=AbstractSequence
    )
    """After commit dispatches for this scope."""

    # ....................... #

    @override
    @classmethod
    def merge(cls, *scopes: Self) -> Self:  # type: ignore[override]
        """Merge multiple transaction scope plans into a single."""

        merged_scope = super().merge(*scopes)  # type: ignore[arg-type]

        merged_after_commit = AbstractSequence.merge(
            *[scope.after_commit for scope in scopes]
        )
        merged_dispatch_after_commit = AbstractSequence.merge(
            *[scope.dispatch_after_commit for scope in scopes]
        )

        routes = {scope.route for scope in scopes if scope.route is not None}

        if len(routes) > 1:
            raise CoreError(
                "Conflicting transaction routes for one operation: " + ", ".join(routes)
            )

        elif len(routes) == 1:
            route = routes.pop()

        else:
            route = None

        return cls(
            route=route,
            after_commit=merged_after_commit,
            dispatch_after_commit=merged_dispatch_after_commit,
            # from outer scope
            before=merged_scope.before,
            wrap=merged_scope.wrap,
            finally_=merged_scope.finally_,
            on_failure=merged_scope.on_failure,
            on_success=merged_scope.on_success,
            dispatch=merged_scope.dispatch,
        )

    # ....................... #

    @override
    def freeze(self) -> FrozenTransactionScope:
        frozen_scope = super().freeze()

        frozen_after_commit = graph_from_sequence(self.after_commit)
        frozen_dispatch_after_commit = pipe_from_sequence(self.dispatch_after_commit)

        return FrozenTransactionScope(
            route=self.route,
            after_commit=frozen_after_commit,
            dispatch_after_commit=frozen_dispatch_after_commit,
            # from outer scope
            before=frozen_scope.before,
            wrap=frozen_scope.wrap,
            finally_=frozen_scope.finally_,
            on_failure=frozen_scope.on_failure,
            on_success=frozen_scope.on_success,
            dispatch=frozen_scope.dispatch,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenScope:
    """Frozen scope plan."""

    before: ExecutionGraph[BeforeStep] = attrs.field(factory=ExecutionGraph)
    """Before steps for this scope."""

    wrap: ExecutionPipeline[MiddlewareStep] = attrs.field(factory=ExecutionPipeline)
    """Wrap steps for this scope."""

    finally_: ExecutionPipeline[FinallyStep] = attrs.field(factory=ExecutionPipeline)
    """Finally steps for this scope."""

    on_failure: ExecutionPipeline[OnFailureStep] = attrs.field(
        factory=ExecutionPipeline
    )
    """On failure steps for this scope."""

    on_success: ExecutionGraph[OnSuccessStep] = attrs.field(factory=ExecutionGraph)
    """On success steps for this scope."""

    dispatch: ExecutionPipeline[DispatchStep] = attrs.field(factory=ExecutionPipeline)
    """Dispatch steps for this scope."""

    # ....................... #

    def resolve(
        self,
        ctx: "ExecutionContext",
        dispatch_resolver: Callable[
            [DispatchStep, "ExecutionContext"],
            OnSuccess[Any, Any],
        ],
    ) -> ResolvedScope:
        resolved_before = resolve_graph(
            self.before,
            ctx,
            resolver=lambda step, ctx: step.factory(ctx),
        )
        resolved_wrap = resolve_pipe(
            self.wrap,
            ctx,
            resolver=lambda step, ctx: step.factory(ctx),
        )
        resolved_finally = resolve_pipe(
            self.finally_,
            ctx,
            resolver=lambda step, ctx: step.factory(ctx),
        )
        resolved_on_failure = resolve_pipe(
            self.on_failure,
            ctx,
            resolver=lambda step, ctx: step.factory(ctx),
        )
        resolved_on_success = resolve_graph(
            self.on_success,
            ctx,
            resolver=lambda step, ctx: step.factory(ctx),
        )
        resolved_dispatch = resolve_pipe(
            self.dispatch,
            ctx,
            resolver=dispatch_resolver,
        )

        return ResolvedScope(
            before=resolved_before,
            wrap=resolved_wrap,
            finally_=resolved_finally,
            on_failure=resolved_on_failure,
            on_success=resolved_on_success,
            dispatch=resolved_dispatch,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FrozenTransactionScope(FrozenScope):
    """Frozen transaction scope plan."""

    route: StrKey | None = None
    """Transaction route for this scope."""

    after_commit: ExecutionGraph[OnSuccessStep] = attrs.field(factory=ExecutionGraph)
    """After commit steps for this scope."""

    dispatch_after_commit: ExecutionPipeline[DispatchStep] = attrs.field(
        factory=ExecutionPipeline
    )
    """After commit dispatches for this scope."""

    # ....................... #

    @override
    def resolve(
        self,
        ctx: "ExecutionContext",
        dispatch_resolver: Callable[
            [DispatchStep, "ExecutionContext"],
            OnSuccess[Any, Any],
        ],
    ) -> ResolvedTransactionScope:
        resolved_scope = super().resolve(ctx, dispatch_resolver)

        resolved_after_commit = resolve_graph(
            self.after_commit,
            ctx,
            resolver=lambda step, ctx: step.factory(ctx),
        )
        resolved_dispatch_after_commit = resolve_pipe(
            self.dispatch_after_commit,
            ctx,
            resolver=dispatch_resolver,
        )

        return ResolvedTransactionScope(
            route=self.route,
            after_commit=resolved_after_commit,
            dispatch_after_commit=resolved_dispatch_after_commit,
            # from outer scope
            before=resolved_scope.before,
            wrap=resolved_scope.wrap,
            finally_=resolved_scope.finally_,
            on_failure=resolved_scope.on_failure,
            on_success=resolved_scope.on_success,
            dispatch=resolved_scope.dispatch,
        )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedScope:
    """Resolved scope plan."""

    before: ExecutionGraph[Before[Any]] = attrs.field(factory=ExecutionGraph)
    """Resolved before hooks for this scope."""

    wrap: ExecutionPipeline[Middleware[Any, Any]] = attrs.field(
        factory=ExecutionPipeline
    )
    """Resolved wrap hooks for this scope."""

    finally_: ExecutionPipeline[Finally[Any, Any]] = attrs.field(
        factory=ExecutionPipeline
    )
    """Resolved finally hooks for this scope."""

    on_failure: ExecutionPipeline[OnFailure[Any]] = attrs.field(
        factory=ExecutionPipeline
    )
    """Resolved on failure hooks for this scope."""

    on_success: ExecutionGraph[OnSuccess[Any, Any]] = attrs.field(
        factory=ExecutionGraph
    )
    """Resolved on success hooks for this scope."""

    dispatch: ExecutionPipeline[OnSuccess[Any, Any]] = attrs.field(
        factory=ExecutionPipeline
    )
    """Resolved dispatch hooks for this scope."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ResolvedTransactionScope(ResolvedScope):
    """Resolved transaction scope plan."""

    route: StrKey | None = None
    """Transaction route for this scope."""

    after_commit: ExecutionGraph[OnSuccess[Any, Any]] = attrs.field(
        factory=ExecutionGraph
    )
    """Resolved after commit hooks for this scope."""

    dispatch_after_commit: ExecutionPipeline[OnSuccess[Any, Any]] = attrs.field(
        factory=ExecutionPipeline
    )
    """Resolved dispatch after commit hooks for this scope."""
