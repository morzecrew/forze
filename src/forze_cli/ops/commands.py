"""``forze ops`` — list, describe and call an application's operations from the command line.

The root ``forze`` CLI mounts the group unbound: each command finds its declaration through
``--app`` or ``FORZE_OPS_APP``. :func:`build_ops_cli` returns the same group with one
declaration bound, for an application's own CLI, and drops ``--app``.
"""

import functools
import inspect
import io
import json
import math
import traceback
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any
from uuid import UUID

import typer

from forze.application.contracts.querying import describe_query_discovery
from forze.application.execution.operations import OperationCatalogEntry

from .call import document_stream, execute
from .contract import EXIT_CONFIG, EXIT_USAGE
from .declaration import OpsApp
from .flags import describe_flags, flag_table

# ----------------------- #

_HELP = (
    "Run your application's operations from the command line, governed as over HTTP or "
    "MCP: input validated, identity verified, tenant resolved, authz and audit applied. "
    "'call' writes one JSON document to stdout and exits with a code scripts can match on."
)

_APP_OPTION: Any = typer.Option(
    None,
    "--app",
    envvar="FORZE_OPS_APP",
    metavar="MODULE:ATTR",
    help="Import string of the OpsApp to use (default: $FORZE_OPS_APP).",
)

# ....................... #


def _load(ref: str | None) -> OpsApp:
    """Import the declaration *ref* names; exits 2 when there is none, 78 when it fails."""

    module, separator, attr = (ref or "").partition(":")

    if not separator or not module or not attr:
        typer.echo(
            f"Name the OpsApp with --app MODULE:ATTR or FORZE_OPS_APP (got {ref!r}).",
            err=True,
        )
        raise typer.Exit(EXIT_USAGE)

    from forze_cli.loader import load_object

    try:
        target = load_object(f"{module}:{attr}")

        if callable(target) and not isinstance(target, OpsApp):
            target = target()

    # The operator needs the traceback to fix their own module; printed without locals.
    except Exception:
        traceback.print_exc()
        raise typer.Exit(EXIT_CONFIG) from None

    if not isinstance(target, OpsApp):
        typer.echo(
            f"{ref!r} is not an OpsApp (got {type(target).__name__}). Expose one, or a "
            "zero-argument callable returning one.",
            err=True,
        )
        raise typer.Exit(EXIT_CONFIG)

    return target


# ....................... #


def _without(command: Callable[..., None], name: str) -> Callable[..., None]:
    """*command* with parameter *name* dropped from its signature and passed as ``None``."""

    signature = inspect.signature(command)

    @functools.wraps(command)
    def bound(**kwargs: Any) -> None:
        command(**kwargs, **{name: None})

    bound.__signature__ = signature.replace(  # type: ignore[attr-defined]
        parameters=[p for p in signature.parameters.values() if p.name != name]
    )

    return bound


def _positive(value: float | None) -> float | None:
    if value is not None and not (0 < value < math.inf):
        raise typer.BadParameter("must be a positive number of seconds")

    return value


def _batch_source(value: str | None) -> str | None:
    if value is not None and value != "-" and not value.startswith("@"):
        raise typer.BadParameter("must be @PATH or - (stdin)")

    return value


def _non_negative(value: float) -> float:
    # A chained comparison refuses NaN too, which every comparison answers False.
    if not 0 <= value < math.inf:
        raise typer.BadParameter("must be a finite number of seconds, zero or more")

    return value


# ....................... #


def _summary(entry: OperationCatalogEntry) -> str:
    descriptor = entry.descriptor

    if descriptor is None:
        return ""

    return descriptor.title or (descriptor.description or "").strip().split("\n", 1)[0]


def _typed_flags(entry: OperationCatalogEntry) -> tuple[list[str] | None, str]:
    """The operation's typed flags, or ``None`` and the reason it takes none."""

    input_type = entry.descriptor.input_type if entry.descriptor is not None else None

    if input_type is None:
        return None, "it takes no input"

    table, reason = flag_table(input_type)

    return (None, reason) if reason is not None else (describe_flags(table), "")


def _description(entry: OperationCatalogEntry, typed_flags: list[str] | None) -> dict[str, Any]:
    descriptor = entry.descriptor
    discovery = descriptor.query_discovery if descriptor is not None else None

    return {
        "op": str(entry.op),
        "kind": str(entry.kind),
        "title": descriptor.title if descriptor is not None else None,
        "description": descriptor.description if descriptor is not None else None,
        "requires_authn": entry.requires_authn,
        "required_permissions": list(entry.required_permissions),
        "supports_idempotency_key": entry.supports_idempotency_key,
        "deadline_seconds": entry.deadline.total_seconds() if entry.deadline else None,
        "typed_flags": typed_flags,
        # A policy that withholds every field describes to an empty sentence.
        "query_discovery": (
            (describe_query_discovery(discovery) or None) if discovery is not None else None
        ),
        "input_schema": descriptor.input_schema() if descriptor is not None else None,
        "output_schema": descriptor.output_schema() if descriptor is not None else None,
    }


def _render(description: dict[str, Any], no_flags_reason: str) -> str:
    lines = [f"{description['op']} ({description['kind']})"]

    for key in ("title", "description"):
        if description[key]:
            lines.append(description[key])

    permissions = ", ".join(description["required_permissions"]) or "none declared"
    deadline = description["deadline_seconds"]
    flags = description["typed_flags"]
    lines += [
        "",
        f"Requires authentication: {'yes' if description['requires_authn'] else 'no'}",
        f"Permissions: {permissions}",
        "Idempotency key: "
        + ("supported" if description["supports_idempotency_key"] else "not supported"),
        f"Deadline: {f'{deadline:g}s' if deadline is not None else 'none'}",
        f"Typed flags: {', '.join(flags) if flags else f'none ({no_flags_reason})'}",
    ]

    if description["query_discovery"]:
        lines.append(f"Querying: {description['query_discovery']}")

    for key, label in (("input_schema", "Input schema"), ("output_schema", "Output schema")):
        schema = description[key]
        lines += ["", f"{label}:", json.dumps(schema, indent=2) if schema else "none"]

    return "\n".join(lines)


# ....................... #


def _group(bound: OpsApp | None, *, name: str) -> typer.Typer:
    group = typer.Typer(name=name, no_args_is_help=True, help=_HELP)

    def declaration(ref: str | None) -> OpsApp:
        return bound if bound is not None else _load(ref)

    def complete_op(ctx: typer.Context, incomplete: str) -> list[str]:
        # The shell reads stdout as candidates and shows stderr: a declaration that cannot
        # load completes nothing, and says nothing.
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                ops = declaration(ctx.params.get("app"))

        except Exception:
            return []

        return [op for op in sorted(ops.exposed) if op.startswith(incomplete)]

    op_argument: Any = typer.Argument(
        ...,
        metavar="OP",
        help="The operation key, e.g. 'orders.get'.",
        autocompletion=complete_op,
    )

    def command(command_name: str, **settings: Any) -> Callable[[Callable[..., None]], Any]:
        def register(fn: Callable[..., None]) -> Any:
            return group.command(command_name, **settings)(
                _without(fn, "app") if bound is not None else fn
            )

        return register

    # ....................... #

    @command("list")
    def list_operations(
        app: str | None = _APP_OPTION,
        as_json: bool = typer.Option(False, "--json", help="Write JSON instead of text."),
    ) -> None:
        """List the exposed operations. Builds no runtime."""

        ops = declaration(app)
        catalog = ops.registry.catalog()
        entries = [catalog[ops.exposed[op]] for op in sorted(ops.exposed)]

        if as_json:
            listing = [
                {"op": str(e.op), "kind": str(e.kind), "title": _summary(e) or None}
                for e in entries
            ]
            typer.echo(json.dumps(listing, indent=2))
            return

        width = max((len(str(e.op)) for e in entries), default=0)

        for e in entries:
            typer.echo(f"{e.op!s:<{width}}  {e.kind!s:<7}  {_summary(e)}".rstrip())

    # ....................... #

    @command("describe")
    def describe(
        op: str = op_argument,
        app: str | None = _APP_OPTION,
        as_json: bool = typer.Option(False, "--json", help="Write JSON instead of text."),
    ) -> None:
        """Describe one exposed operation: its contract, guards and schemas. Builds no runtime."""

        ops = declaration(app)
        key = ops.exposed.get(op)

        if key is None:
            typer.echo(f"No exposed operation {op!r}; 'list' shows them.", err=True)
            raise typer.Exit(EXIT_USAGE)

        entry = ops.registry.catalog()[key]
        flags, no_flags_reason = _typed_flags(entry)
        description = _description(entry, flags)
        typer.echo(
            json.dumps(description, indent=2) if as_json else _render(description, no_flags_reason)
        )

    # ....................... #

    # Typed flags depend on the operation, which Click has not read when it parses: they reach
    # the command unparsed, and only flags no call option claims ever do.
    @command("call", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
    def call(
        ctx: typer.Context,
        op: str = op_argument,
        app: str | None = _APP_OPTION,
        raw_input: str | None = typer.Option(
            None,
            "--input",
            metavar="JSON|@PATH|-",
            help="The input object: inline JSON, @PATH to read a file, or - for stdin.",
        ),
        batch: str | None = typer.Option(
            None,
            "--batch",
            metavar="@PATH|-",
            callback=_batch_source,
            help="Call once per JSONL line of a file or stdin, in one runtime; one line out per "
            "call, stopping at the first failure.",
        ),
        tenant: UUID | None = typer.Option(
            None,
            "--tenant",
            help="Request a tenant; validated against the principal, never trusted raw.",
        ),
        idempotency_key: str | None = typer.Option(
            None,
            "--idempotency-key",
            metavar="KEY",
            help="Replay the stored result of an earlier call with this key.",
        ),
        timeout: float | None = typer.Option(
            None,
            "--timeout",
            metavar="SECONDS",
            callback=_positive,
            help="Bound the operation; only ever tightens its declared deadline.",
        ),
        grace: float = typer.Option(
            10.0,
            "--grace",
            metavar="SECONDS",
            callback=_non_negative,
            help="How long a first SIGINT or SIGTERM waits for the call before cancelling it.",
        ),
        correlation_id: UUID | None = typer.Option(None, "--correlation-id"),
        causation_id: UUID | None = typer.Option(None, "--causation-id"),
        api_key_file: Path | None = typer.Option(
            None,
            "--api-key-file",
            metavar="PATH",
            dir_okay=False,
            help="Read the API key from a file instead of the environment.",
        ),
        pretty: bool = typer.Option(False, "--pretty", help="Indent the result."),
    ) -> None:
        """Call one exposed operation and write its result, or the error, as one JSON document.

        An input of flat scalars also takes typed flags after OP; 'describe' lists them.
        """

        with document_stream() as out:

            def emit(line: str) -> None:
                # A batch's lines are read as they come, not when the batch ends.
                out.write(line + "\n")
                out.flush()

            ops = declaration(app)
            document, code = execute(
                ops,
                op=op,
                raw_input=raw_input,
                flags=ctx.args,
                batch=batch,
                tenant=tenant,
                idempotency_key=idempotency_key,
                timeout=timeout,
                grace=grace,
                correlation_id=correlation_id,
                causation_id=causation_id,
                api_key_file=api_key_file,
                pretty=pretty,
                emit=emit,
            )

            if document is not None:
                out.write(document + "\n")

        raise typer.Exit(code)

    return group


# ....................... #

ops_cli = _group(None, name="ops")
"""The unbound group the root ``forze`` CLI mounts."""


def build_ops_cli(app: OpsApp, *, name: str = "ops") -> typer.Typer:
    """The ``ops`` group with *app* bound; see :func:`forze_cli.ops.build_ops_cli`."""

    return _group(app, name=name)
