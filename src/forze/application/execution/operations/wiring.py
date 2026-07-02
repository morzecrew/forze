"""Dry-run wiring check for a frozen operation registry.

Missing dependencies otherwise surface only when an operation is *first* invoked in a
live scope: the handler factory runs, asks for a port that was never registered, and
``ProviderStore.get_provider`` raises a configuration error. A rarely-hit operation can
pass startup and CI and then fail on its first production call.

:func:`check_wiring` closes that gap by eagerly resolving every registered operation
against a throwaway context — the same code path a real call takes — and collecting the
failures instead of stopping at the first. Because it routes through
:meth:`FrozenOperationRegistry.resolve`, one call per operation transitively builds the
handler, every hook and transaction-scope stage, and (recursively) any saga dispatch
targets, and it replicates the read-only bind a QUERY operation is built under — so an
eagerly-acquired write port in a QUERY factory is caught exactly as it would be at call
time. Lifecycle steps are *not* exercised: they open real connections at startup and are
validated by opening an actual scope.

This is an opt-in diagnostic. It runs every factory, which is safe only under the
framework contract that factories are synchronous builders that defer all I/O to call
time; a factory that violates that contract will actually perform its I/O here (and,
since startup is skipped, most likely raise — landing in the non-configuration bucket
rather than reaching a real backend).

Typical use is a test or a dev/staging startup gate::

    report = check_wiring(registry, lambda: context_from_modules(MockDepsModule(...)))
    report.raise_if_failed()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import attrs

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from ..context import ExecutionContext
    from .facade import OperationFacade, OperationFacadeFactory
    from .registry import FrozenOperationRegistry

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class WiringFailure:
    """A single operation that failed to resolve during a dry-run wiring check."""

    op: StrKey
    """The operation whose resolution failed."""

    kind: ExceptionKind
    """Kind of the raised error. ``CONFIGURATION`` is the wiring bucket (missing/duplicate
    dependency, read-only guard, two-phase mismatch); anything else is an unexpected build
    error surfaced rather than swallowed."""

    message: str
    """The error summary."""

    code: str | None = None
    """The error code when the failure is a :class:`CoreException` (e.g. ``core.configuration``)."""

    @property
    def is_wiring(self) -> bool:
        """Whether this is a configuration (wiring) failure, as opposed to an unexpected error."""

        return self.kind is ExceptionKind.CONFIGURATION


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class WiringReport:
    """Result of a dry-run wiring check over a set of operations."""

    checked: tuple[StrKey, ...]
    """Every operation that was resolved."""

    failures: tuple[WiringFailure, ...]
    """Failures, one per operation that did not resolve cleanly."""

    @property
    def ok(self) -> bool:
        """Whether every checked operation resolved without error."""

        return not self.failures

    def raise_if_failed(self) -> None:
        """Raise a single configuration error aggregating every failure.

        Aggregating (rather than raising the first) means a test or startup gate reports the
        whole list of broken operations in one pass, not one miss at a time.
        """

        if not self.failures:
            return

        lines = "\n".join(
            f"  - {failure.op}: [{failure.kind}] {failure.message}"
            for failure in self.failures
        )
        raise exc.configuration(
            f"Operation wiring check failed for {len(self.failures)} of "
            f"{len(self.checked)} operation(s):\n{lines}"
        )


# ....................... #


def check_wiring(
    registry: "FrozenOperationRegistry",
    context_factory: "Callable[[], ExecutionContext]",
    *,
    ops: "Iterable[StrKey] | None" = None,
) -> WiringReport:
    """Dry-run resolve every operation to catch missing/misrouted dependencies.

    Args:
        registry: The frozen operation registry to check.
        context_factory: Mints one throwaway :class:`ExecutionContext` for the pass — e.g.
            ``lambda: ExecutionContext(deps=runtime.deps.resolve())`` or, in tests,
            ``lambda: context_from_modules(MockDepsModule(...))``. It must resolve from the
            same frozen deps the runtime uses, or the check will not reflect real wiring.
        ops: Operations to check; defaults to every operation in ``registry.handlers``.

    Returns:
        A :class:`WiringReport`. Call :meth:`WiringReport.raise_if_failed` to fail fast.
    """

    keys = tuple(ops) if ops is not None else tuple(registry.handlers)
    ctx = context_factory()

    failures: list[WiringFailure] = []
    for op in keys:
        try:
            registry.resolve(op, ctx)
        except CoreException as error:
            failures.append(
                WiringFailure(
                    op=op,
                    kind=error.kind,
                    message=error.summary,
                    code=error.code,
                )
            )
        except Exception as error:  # noqa: BLE001 — surface, don't swallow, unexpected build errors
            failures.append(
                WiringFailure(
                    op=op,
                    kind=ExceptionKind.INTERNAL,
                    message=str(error),
                )
            )

    return WiringReport(checked=keys, failures=tuple(failures))


# ....................... #


def check_facade_factory_wiring(
    factory: "OperationFacadeFactory[OperationFacade]",
) -> WiringReport:
    """Convenience wrapper: check a facade factory's registry using its own context factory."""

    return check_wiring(factory.registry, factory.ctx_factory)
