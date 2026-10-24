"""Run one ``forze ops call`` as a governed invocation and report it as one document.

The MCP boundary's sequence through the shared ingress: validate the input, verify the
identity, resolve the tenant, bind the envelope, then ``run_operation`` on the runtime's
scope context. Every failure after the declaration loads becomes the error object, never a
traceback, so a script reads stdout and the exit code and nothing else.
"""

import asyncio
import json
import os
import signal
import sys
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any, TextIO, cast
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import COMMIT_AMBIGUOUS_CODE
from forze.application.execution import DeploymentProfile, ExecutionContext, ExecutionRuntime
from forze.application.execution.context import InvocationMetadata
from forze.application.execution.context.commit_state import (
    commit_started,
    reset_commit_started,
)
from forze.application.execution.ingress import (
    authenticate_api_key,
    invoke_at_ingress,
    mint_invocation_metadata,
    resolve_principal_tenant,
    validate_operation_input,
)
from forze.application.execution.operations import OperationCatalogEntry
from forze.base.exceptions import (
    CoreException,
    error_envelope,
    exc,
    unhandled_error_envelope,
)
from forze.base.logging import Logger, log_server_error, log_withheld_details

from .contract import (
    EXIT_INTERNAL,
    EXIT_OK,
    EXIT_USAGE,
    error_document,
    exit_code,
    interrupted_document,
    interrupted_exit_code,
    result_document,
)
from .declaration import ApiKeyIdentity, OpsApp, OpsIdentity, StaticIdentity

# ----------------------- #

_logger = Logger("cli.errors")

type _Outcome = tuple[str, int]
"""The one stdout document and the exit code."""

# ....................... #


class _Usage(Exception):
    """A call the CLI refuses as misused: reported as *error*, but exiting 2."""

    def __init__(self, error: CoreException) -> None:
        super().__init__(error.summary)
        self.error = error


def _usage(summary: str, code: str) -> _Usage:
    return _Usage(exc.validation(summary, code=code))


# ....................... #


@contextmanager
def document_stream() -> Iterator[TextIO]:
    """Point stdout at stderr while a call runs; yield the real stdout for the one document.

    An application's logging may write to stdout, and the application configures it, so
    the descriptor itself is redirected: logs, prints and a library's stray output all land
    on stderr.
    """

    original = sys.stdout
    original.flush()

    try:
        out_fd, err_fd = original.fileno(), sys.stderr.fileno()

    # A captured stream (a test runner's) has no descriptor to redirect.
    except (AttributeError, OSError, ValueError):
        sys.stdout = sys.stderr

        try:
            yield original

        finally:
            sys.stdout = original

        return

    saved = os.dup(out_fd)
    os.dup2(err_fd, out_fd)
    sys.stdout = sys.stderr

    try:
        with open(saved, "w", encoding="utf-8", closefd=False) as document:
            yield document

    finally:
        sys.stdout = original

        try:
            # What the call left buffered belongs on stderr, where the descriptor still points.
            original.flush()

        finally:
            os.dup2(saved, out_fd)
            os.close(saved)


# ....................... #


def execute(
    ops: OpsApp,
    *,
    op: str,
    raw_input: str | None,
    tenant: UUID | None,
    idempotency_key: str | None,
    timeout: float | None,
    grace: float,
    correlation_id: UUID | None,
    causation_id: UUID | None,
    api_key_file: Path | None,
    pretty: bool,
) -> _Outcome:
    """Run *op* on *ops* and return the one stdout document with its exit code."""

    # Minted before anything can fail, so every error object carries both ids.
    metadata = mint_invocation_metadata(correlation_id=correlation_id, causation_id=causation_id)
    repeatable = False

    try:
        entry = _exposed_entry(ops, op)
        repeatable = entry.is_read_only or idempotency_key is not None
        _check_options(ops.identity, entry, tenant, idempotency_key, api_key_file)
        args = validate_operation_input(
            entry.descriptor,
            _read_input(raw_input, entry),
            code="cli_invalid_input",
            forbid_unknown=True,
        )
        api_key = _read_api_key(ops.identity, api_key_file)

        runtime = ops.runtime()

        if runtime.deployment is not DeploymentProfile.SERVERLESS:
            raise exc.configuration(
                f"forze ops runs only on a SERVERLESS runtime, not {runtime.deployment.name}",
                code="cli_runtime_not_serverless",
            )

        return asyncio.run(
            _run(
                ops,
                runtime,
                entry,
                args,
                metadata=metadata,
                tenant=tenant,
                api_key=api_key,
                idempotency_key=idempotency_key,
                timeout=timeout,
                grace=grace,
                pretty=pretty,
                repeatable=repeatable,
            )
        )

    except _Usage as usage:
        return error_document(error_envelope(usage.error), metadata), EXIT_USAGE

    except CoreException as error:
        return _failure(error, metadata, repeatable=repeatable)

    # Before the signal handlers are installed, or after they are removed.
    except KeyboardInterrupt:
        return (
            interrupted_document(signal.SIGINT, metadata),
            interrupted_exit_code(signal.SIGINT),
        )

    except Exception as error:
        return _crash(error, metadata)


# ....................... #


def _failure(error: CoreException, metadata: InvocationMetadata, *, repeatable: bool) -> _Outcome:
    envelope = error_envelope(error)

    if envelope.server_error:
        log_server_error(_logger, error, core=error)

    log_withheld_details(_logger, error)

    return error_document(envelope, metadata), exit_code(envelope, repeatable=repeatable)


def _crash(error: Exception, metadata: InvocationMetadata) -> _Outcome:
    log_server_error(_logger, error)

    return error_document(unhandled_error_envelope(), metadata), EXIT_INTERNAL


# ....................... #


def _exposed_entry(ops: OpsApp, op: str) -> OperationCatalogEntry:
    key = ops.exposed.get(op)

    # Unexposed reads as unknown: the CLI has no such command, whatever the registry holds.
    if key is None:
        raise _usage(
            f"No exposed operation {op!r}; 'forze ops list' shows them",
            "cli_unknown_operation",
        )

    return ops.registry.catalog()[key]


def _check_options(
    identity: OpsIdentity,
    entry: OperationCatalogEntry,
    tenant: UUID | None,
    idempotency_key: str | None,
    api_key_file: Path | None,
) -> None:
    # A script must never believe a retry is safe when the operation would re-run.
    if idempotency_key is not None and not entry.supports_idempotency_key:
        raise _usage(
            f"Operation {str(entry.op)!r} does not support an idempotency key",
            "cli_idempotency_unsupported",
        )

    api_key = isinstance(identity, ApiKeyIdentity)

    if tenant is not None and not api_key:
        raise _usage(
            "--tenant needs an API-key identity: there is no principal to validate it against",
            "cli_tenant_unverifiable",
        )

    if api_key_file is not None and not api_key:
        raise _usage(
            "--api-key-file needs an API-key identity",
            "cli_api_key_file_unused",
        )


def _read_input(raw_input: str | None, entry: OperationCatalogEntry) -> dict[str, Any]:
    input_type = entry.descriptor.input_type if entry.descriptor is not None else None

    if raw_input is None:
        return {}

    if input_type is None:
        raise _usage(
            f"Operation {str(entry.op)!r} takes no input",
            "cli_input_not_accepted",
        )

    if raw_input == "-":
        text = sys.stdin.read()

    elif raw_input.startswith("@"):
        try:
            text = Path(raw_input[1:]).read_text(encoding="utf-8")

        except OSError as error:
            raise _usage(
                f"Cannot read --input file {raw_input[1:]!r}: {error.strerror}",
                "cli_input_unreadable",
            ) from error

    # argv is visible in ``ps`` and shell history.
    elif _declares_secret(input_type.model_json_schema()):
        raise _usage(
            f"Operation {str(entry.op)!r} takes a secret: pass its input on stdin "
            "(--input -) or from a file (--input @PATH), not inline",
            "cli_inline_secret",
        )

    else:
        text = raw_input

    # The text is never echoed: it may hold the secret the caller meant to protect.
    try:
        value = json.loads(text)

    except ValueError as error:
        raise _usage("--input is not valid JSON", "cli_malformed_input") from error

    if not isinstance(value, dict):
        raise _usage("--input must be a JSON object", "cli_malformed_input")

    return value  # pyright: ignore[reportUnknownVariableType]


def _declares_secret(schema: Any) -> bool:
    """Whether a JSON Schema declares a ``SecretStr`` or ``SecretBytes`` anywhere in it."""

    if isinstance(schema, dict):
        return schema.get("format") == "password" or any(  # pyright: ignore[reportUnknownMemberType]
            _declares_secret(value)
            for value in schema.values()  # pyright: ignore[reportUnknownVariableType]
        )

    if isinstance(schema, list):
        return any(_declares_secret(value) for value in schema)  # pyright: ignore[reportUnknownVariableType]

    return False


def _read_api_key(identity: OpsIdentity, api_key_file: Path | None) -> str | None:
    if not isinstance(identity, ApiKeyIdentity):
        return None

    if api_key_file is None:
        raw = os.environ.get(identity.env, "")

    else:
        try:
            raw = api_key_file.read_text(encoding="utf-8")

        except OSError as error:
            raise _usage(
                f"Cannot read --api-key-file {str(api_key_file)!r}: {error.strerror}",
                "cli_api_key_unreadable",
            ) from error

    if not raw.strip():
        source = (
            f"--api-key-file {str(api_key_file)!r} is empty"
            if api_key_file
            else f"set {identity.env} or pass --api-key-file"
        )

        raise exc.authentication(f"No API key: {source}", code="cli_api_key_missing")

    return raw


# ....................... #


async def _run(
    ops: OpsApp,
    runtime: ExecutionRuntime,
    entry: OperationCatalogEntry,
    args: Any,
    *,
    metadata: InvocationMetadata,
    tenant: UUID | None,
    api_key: str | None,
    idempotency_key: str | None,
    timeout: float | None,
    grace: float,
    pretty: bool,
    repeatable: bool,
) -> _Outcome:
    """Enter the runtime's scope and run the call, under the two-signal shape.

    The first SIGINT or SIGTERM waits up to *grace* for the call to finish, because
    cancelling a commit leaves its outcome unknown; a second one, or the grace running out,
    cancels it. Shutdown runs either way.
    """

    loop = asyncio.get_running_loop()
    # ``asyncio.run`` drives this coroutine as a task.
    task = cast(asyncio.Task[Any], asyncio.current_task())
    signals: list[int] = []
    grace_timer: asyncio.TimerHandle | None = None
    outcome: _Outcome | None = None

    def on_signal(signum: int) -> None:
        nonlocal grace_timer
        signals.append(signum)

        if len(signals) > 1:
            task.cancel()

        # Once the call is over, a first signal waits for the shutdown: only a second one
        # cuts it short.
        elif outcome is None:
            grace_timer = loop.call_later(grace, task.cancel)

    # Left installed until ``asyncio.run`` closes the loop, which removes them: a signal
    # while the loop winds down must not turn a finished call into an interrupted one.
    _install_signal_handlers(loop, on_signal)

    try:
        async with runtime.scope():
            if signals:
                outcome = _interrupted(signals[0], metadata)

            else:
                outcome = await _attempt(
                    ops,
                    runtime.get_context(),
                    entry,
                    args,
                    metadata=metadata,
                    tenant=tenant,
                    api_key=api_key,
                    idempotency_key=idempotency_key,
                    timeout=timeout,
                    pretty=pretty,
                    repeatable=repeatable,
                    signals=signals,
                )

            # The call is over: only a second signal may still cut the shutdown short.
            if grace_timer is not None:
                grace_timer.cancel()

    except asyncio.CancelledError:
        if not signals:
            raise

        _uncancel(task)

        if outcome is None:
            outcome = _interrupted(signals[0], metadata)

    except Exception as error:
        # Startup failed: the caller reports it. Shutdown failed after the call: its own
        # outcome stands, and the failure goes to stderr.
        if outcome is None:
            raise

        log_server_error(_logger, error)

    finally:
        if grace_timer is not None:
            grace_timer.cancel()

    return outcome


async def _attempt(
    ops: OpsApp,
    ctx: ExecutionContext,
    entry: OperationCatalogEntry,
    args: Any,
    *,
    metadata: InvocationMetadata,
    tenant: UUID | None,
    api_key: str | None,
    idempotency_key: str | None,
    timeout: float | None,
    pretty: bool,
    repeatable: bool,
    signals: list[int],
) -> _Outcome:
    """Resolve the identity and run the operation; the outcome is built inside the scope,
    under its denial posture."""

    # A transaction the startup committed must not read as this call's commit.
    reset_commit_started()

    try:
        authn, bound_tenant = await _identity(ctx, ops.identity, tenant=tenant, api_key=api_key)
        result = await invoke_at_ingress(
            ops.registry,
            ctx,
            entry.op,
            args,
            authn=authn,
            tenant=bound_tenant,
            metadata=metadata,
            idempotency_key=idempotency_key,
            deadline=timeout,
        )

        return result_document(entry.descriptor, result, pretty=pretty), EXIT_OK

    except asyncio.CancelledError:
        if not signals:
            raise

        # Read before the scope's shutdown, whose hooks may run an operation and clear it.
        torn = commit_started()
        task = asyncio.current_task()

        if task is not None:
            _uncancel(task)

        if not torn:
            return _interrupted(signals[0], metadata)

        error = exc.internal(
            f"Operation {str(entry.op)!r} was cancelled at or after its commit point; the "
            "commit may have landed. Reconcile before re-running.",
            code=COMMIT_AMBIGUOUS_CODE,
        )

        return _failure(error, metadata, repeatable=repeatable)

    except _Usage as usage:
        return error_document(error_envelope(usage.error), metadata), EXIT_USAGE

    except CoreException as error:
        return _failure(error, metadata, repeatable=repeatable)

    except Exception as error:
        return _crash(error, metadata)


async def _identity(
    ctx: ExecutionContext,
    identity: OpsIdentity,
    *,
    tenant: UUID | None,
    api_key: str | None,
) -> tuple[AuthnIdentity | None, TenantIdentity | None]:
    if isinstance(identity, StaticIdentity):
        return identity.authn, identity.tenant

    if not isinstance(identity, ApiKeyIdentity) or api_key is None:
        return None, None

    result = await authenticate_api_key(ctx, identity.authn_spec, api_key)
    route = identity.tenancy_route or str(identity.authn_spec.name)

    try:
        bound = await resolve_principal_tenant(ctx, result, route=route, requested=tenant)

    except CoreException as error:
        # Nothing can validate the requested tenant: a misused option, not a denial.
        if error.code == "tenant_request_unverifiable":
            raise _Usage(error) from error

        raise

    return result.identity, bound


# ....................... #


def _interrupted(signum: int, metadata: InvocationMetadata) -> _Outcome:
    return interrupted_document(signum, metadata), interrupted_exit_code(signum)


def _uncancel(task: asyncio.Task[Any]) -> None:
    # The CLI absorbed the cancellations it requested; leave none pending on the task.
    while task.cancelling():
        task.uncancel()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, handler: Any) -> None:
    for signum in (signal.SIGINT, signal.SIGTERM):
        # Not the main thread, or a platform without loop signal support.
        with suppress(NotImplementedError, RuntimeError, ValueError):
            loop.add_signal_handler(signum, handler, signum)
