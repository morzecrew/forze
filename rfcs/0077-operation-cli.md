# RFC 0077 — Operation CLI: governed operations from the command line

- **Status:** ✅ Complete (P1, P2). P1, the shared ingress core, landed on 2026-10-23, and
  P2, `forze ops`, on 2026-10-24; the "As built" notes in §5.7 and §5.8 record where each
  departs from the plan. P3 stays demand-gated (§12); its commit-ambiguity item was built on
  2026-10-27 (§5.6, "As built (P3)"). The three questions were answered on
  2026-10-22 (§10, decisions 14, 16 and 17).
- **Scope:** A `forze ops` command group in the existing `forze_cli` package. It lists,
  describes and invokes the operations of an application's frozen registry, in process,
  through the same governed path HTTP and MCP use. The RFC also covers the transport-agnostic
  boundary pieces this group needs, lifted into core so that the CLI is not their fourth copy.
  It settles:
  - the declaration an application exposes;
  - how identity is chosen and verified;
  - the invocation envelope: correlation, idempotency, deadline;
  - the output and exit-code contract;
  - which runtimes a command may run.

  It does **not** cover a remote client of a deployed service, an interactive shell, or
  development tasks, which the `justfile` owns.
- **Related:**
  - [`src/forze_cli/app.py`](../src/forze_cli/app.py): its docstring already leaves "room for
    `ops`" (`:6`);
  - [`src/forze_mcp/dispatch.py`](../src/forze_mcp/dispatch.py),
    [`projection.py`](../src/forze_mcp/projection.py),
    [`auth.py`](../src/forze_mcp/auth.py): the boundary sequence this RFC generalises;
  - [`src/forze/application/execution/operations/`](../src/forze/application/execution/operations/):
    `OperationCatalogEntry`, `FrozenOperationRegistry.catalog()`, `run_operation`;
  - [`src/forze/application/execution/runtime.py`](../src/forze/application/execution/runtime.py):
    `DeploymentProfile.SERVERLESS`;
  - [`src/forze/base/exceptions/envelope.py`](../src/forze/base/exceptions/envelope.py):
    `error_envelope`;
  - RFC 0022 (the acknowledgement-flag shape the static identity borrows);
  - RFC 0023 (the in-process tool bridge, the same projection pointed inward);
  - RFC 0059 (non-disclosing denials, which decide what an error reveals);
  - RFC 0060 (audit, where the actor comes from);
  - RFC 0061 (production posture, P3 of which names a `forze check-posture` entry);
  - RFC 0064 (principal-scoped idempotency claims);
  - RFC 0074 (the two-signal drain shape the interrupt handling follows).
- **Origin:** A maintainer question on 2026-10-22: is a CLI worth building next to the HTTP,
  websocket and MCP surfaces? Admin work is done today with ad-hoc scripts: requeuing failed
  durable runs, tenant administration, backfills, one-off fixes. Each script builds a runtime
  and then either calls handlers directly or goes straight to the database. That is outside
  authz, tenancy, read-only enforcement, idempotency and audit.

---

## 1. Summary

- **`forze ops list | describe | call`** is pointed at an `OpsApp` declaration the
  application exposes, through `--app module:attr` or `FORZE_OPS_APP`.
  `build_ops_cli(app)` mounts the same group inside an application's own Typer CLI.
- **Every call is a governed invocation.** The input is validated against the operation's
  DTO, the identity is verified, the tenant is resolved and the metadata is minted. Then
  `run_operation` runs on the runtime's scope context. This is the MCP boundary's sequence,
  unchanged.
- **Identity is a required, explicit choice.** There are three:
  - a verified API key, read from the environment or a file and never from argv;
  - an anonymous binding;
  - a static identity, behind a required acknowledgement.

  The choice itself has no default, and there is no `--as` impersonation.
- **Exposure follows MCP:**
  - queries by default;
  - commands only with `include_writes=True`;
  - an optional allowlist;
  - sensitive read models refused.
- **A published machine contract:**
  - after the declaration loads, exactly one JSON document on stdout: the result, or a
    masked error object carrying the invocation ids. Everything else the application writes
    goes to stderr;
  - an exit-code table derived from the error envelope's kind. Exit 75, "retry", is reserved
    for failures that are safe to repeat.
- **An interrupt never hides a possible commit.** The first signal waits a grace period for
  the call to finish. A cancel after the commit point reports `commit_ambiguous`.
- **A command runs only on a runtime that hosts no long-running step.** That means a
  `SERVERLESS`-profiled runtime: assembly refuses steps declared `requires_long_running`,
  and the drain window is zero. The guard is that declared marker, which steps set themselves;
  it is not detected. The profile keeps its name (§10).
- **P1 lifts the boundary sequence into `forze.application.execution.ingress`:**
  - input validation;
  - exposure selection;
  - bind-and-run;
  - API-key and tenant resolution.

  `forze_mcp` moves onto it with no behaviour change, so the shared code has two consumers
  from its first day.

## 2. Motivation

**Operators have no governed entry point.**
- Governance lives in operation plans and reads the invocation context:
  - authn through `AuthnRequired` (`hooks/authn/plans.py:21`);
  - tenancy through `TenantRequired` (`hooks/tenancy/plans.py:18`);
  - authz, audit and idempotency hooks the same way.
- `run_operation` resolves the plan (`execution/operations/run/invoke.py:268`). The resolved
  operation then adds the drain gate (`:180-183`), read-only enforcement (`:133`) and the
  deadline (`:71`, `:94`).
- Code that calls a handler or a port directly skips all of it.
- The two existing remote surfaces do not fit operator work:
  - HTTP needs a route per operation and a deployed service;
  - MCP exposes queries by default and is shaped for agents.
- So a one-off fix becomes a script that holds database credentials and leaves no audit row.
- The operations an operator needs already exist as registry entries. Two examples:
  - the durable admin port: `requeue_failed`, `list_runs`;
  - the tenancy admin kit.

  What is missing is the front door.

**A new surface built the obvious way would be the fourth copy.** The boundary sequence
already exists more than once.

| Piece | Copies today |
|---|---|
| Validate raw input into the DTO, errors sanitized | MCP `build_args` (`forze_mcp/dispatch.py:36`), Socket.IO `_parse_payload`, the realtime WS route, agent tools `_validated_args` (`forze_kits/integrations/agent_tools/dispatch.py:152`) |
| Choose exposed operations, refuse sensitive ones | MCP `exposed_operations` (`forze_mcp/projection.py:17`) plus `registration.py:359-370`, FastAPI `_attach.py:926`, agent tools `operation_tools` (`agent_tools/projection.py:76`) |
| Mint metadata, bind identity, run | MCP `invoke_operation` (`forze_mcp/dispatch.py:71`), FastAPI middleware plus `_operation_runner` (`forze_fastapi/routes/_attach.py:290`), WS (`forze_fastapi/realtime/ws.py:780`) |
| Split `prefix:secret`, resolve the principal's tenant | MCP `_split_api_key` and `_resolve_tenant` (`forze_mcp/auth.py:74`, `:92`), FastAPI `_resolve_header_api_key_authn` and `resolve_tenant_identity` (`forze_fastapi/security/resolvers.py:200`, `:262`) |

The copies already disagree:
- **Unknown argument names.** Agent tools refuse them. MCP's `build_args` passes them to
  `model_validate`, which drops them silently unless the DTO forbids extras.
- **Socket.IO's event path** (`forze_socketio/routing.py:373-389`):
  - it binds identity but no `InvocationMetadata`;
  - it calls the resolved operation directly, not through `run_operation`.

The CLI cannot reuse any copy:
- `forze_mcp` refuses import without the `mcp` extra: `__init__.py` calls `require_mcp()`.
- `forze_kits` admits only `forze_fastapi` as an importer.
- `forze_identity` admits none (`pyproject.toml`, "Only sanctioned consumers may import
  forze_kits" and "Nothing imports forze_identity").

Without a shared home, the CLI copies the sequence again, and the next surface copies it a
fifth time.

## 3. Current state

All of this was verified against the tree at `4ad063818`.

- **The CLI.**
  - `forze = "forze_cli:main"` is the console script. `main()` checks for Typer and prints
    an install hint for the `cli` extra (`src/forze_cli/__init__.py:15-29`).
  - The root app sets `pretty_exceptions_show_locals=False`, so a crash never prints
    secrets held in locals (`app.py:20-35`).
  - Two groups exist: `dst` and `mock`.
  - `load_object(ref)` resolves `module:attr` with the cwd on `sys.path`
    (`loader.py:66`).
  - Nothing in the package runs asyncio: `dst` drives a synchronous engine and `mock` hands
    off to uvicorn.
  - Output is plain text. Failures exit 1 and usage errors exit 2.
- **The catalog.** `FrozenOperationRegistry.catalog()` returns one `OperationCatalogEntry`
  per handler (`registries.py:618`). An entry carries:
  - `op` and `kind`;
  - `descriptor`, which may be `None`;
  - `supports_idempotency_key`;
  - `required_permissions` and `requires_authn`, both declared-hook introspection;
  - `deadline`;
  - `is_read_only` (`descriptors.py:98-146`).

  `OperationDescriptor` carries:
  - `input_type`, `output_type`, `title`, `description`, `tags`;
  - `sensitive` and `query_discovery`;
  - `input_schema()` and `output_schema()` (`descriptors.py:76`, `:83`).

  `resolve` on an unknown key raises `internal`, so a boundary checks the catalog first.
- **Dispatch.**
  - `run_operation(registry, op, args, ctx)` takes an already-validated DTO
    (`invoke.py:268`).
  - Identity is bound with `ctx.inv_ctx.bind(metadata=, authn=, tenant=)`
    (`context/invocation.py:322`).
  - The idempotency key is bound with `bind_idempotency` (`:207`) and the deadline with
    `bind_deadline` (`:194`).
  - `InvocationMetadata` has `execution_id`, `correlation_id` and `causation_id`
    (`:30`). There is no field naming the calling surface: not there, not in log fields, not
    in `AuditEntry` (`contracts/audit/value_objects.py:53`).
- **The runtime.**
  - `ExecutionRuntime.scope()` creates the context, runs lifecycle startup, yields, then
    drains and shuts down (`runtime.py:625`).
  - `get_context()` returns the scope's single context (`:474`).
  - Building a context per call is unsupported.
  - Posture is enforced at construction (`:276-279`).
  - Under `DeploymentProfile.SERVERLESS`, assembly refuses any lifecycle step declared
    `requires_long_running` (`:295-310`), and `build_runtime` sets both drain defaults to zero
    (`assemble.py:190-198`). Both step markers are declared by the step's author, not detected.
- **Errors.**
  - `error_envelope(exc)` applies masking and the denial posture (`envelope.py:142`).
  - Every kind has an egress policy with `retryable`
    (`src/forze/base/exceptions/egress.py:92`). Today the retryable kinds are `concurrency`,
    `throttled` and `infrastructure`.
  - A deadline that fires after the commit point is reported as `internal` /
    `commit_ambiguous`, so it is not retried (`invoke.py:104-122`). That check catches only
    `TimeoutError`. A task cancelled during a commit reports nothing.
  - `unhandled_error_envelope()` covers everything that is not a `CoreException`
    (`envelope.py:199`).
- **Identity.**
  - `AuthnPort.authenticate_with_api_key` runs the full verifier, resolver and eligibility
    pipeline with no transport in it (`contracts/authn/ports/authn.py:39`, orchestrator
    `integrations/authn/orchestrator.py:309`).
  - Tenancy resolves from the principal with `ctx.tenancy.resolver(route)
    .resolve_from_principal(principal_id, requested_tenant_id=)`. A raw tenant hint is
    trusted only from a verified credential (`resolvers.py:262-300`).
  - `PrincipalKind` is `user | service` (`contracts/authz/types.py:5`).
  - There is no superuser or system bypass. Background workers bind a tenant and no principal
    (`forze_kits/integrations/durable/scheduler.py:139`).
  - `inv_ctx.bind` accepts any `AuthnIdentity` it is given, verified or not.
- **Skills census.** `forze_cli` is doctrine D3, "Maintainer tooling … nothing here is reached
  from an application" (`tools/skills_check/coverage.toml:161`). This RFC makes that false.

## 4. Goals / Non-goals

**Goals**

- An operator, cron job, Kubernetes Job or CI step can run any exposed operation with the
  same governance as an HTTP or MCP caller of that operation.
- Who acted is a verified principal wherever the deployment allows it, and is recorded the
  same way any other surface records it.
- The output and exit codes are a stable contract that scripts can rely on across releases.
- One boundary implementation shared by MCP and the CLI. FastAPI and the realtime routes can
  move onto it later.
- An application can ship its own branded CLI with the group mounted, without forking it.

**Non-goals**

- **A remote client.** Calling a deployed service over HTTP or MCP is a different product,
  with a different trust model and no access to infrastructure credentials (§8).
- **An interactive shell or REPL.**
- **Development tasks.** Tests, quality and docs stay in the `justfile`, as `app.py`
  already states.
- **Framework maintenance commands.** Posture checks, schema attestation and migrations are
  not operations. They get their own groups if they ship: RFC 0061 P3, RFC 0029.
- **Generated per-operation commands with typed flags** in the first version (§8, §12).

## 5. Design

### 5.1 The declaration

```python
@attrs.frozen(kw_only=True)
class OpsApp:
    registry: FrozenOperationRegistry
    runtime: Callable[[], ExecutionRuntime]     # built lazily, once per `call`
    identity: OpsIdentity                       # required — §5.3
    include_writes: bool = False
    operations: Iterable[StrKey] | None = None  # optional allowlist
```

- **Location.** It lives in `forze_cli.ops`, which imports attrs and core only at module
  level. `build_ops_cli` imports Typer inside its body. An application module that declares
  `OpsApp` therefore stays importable without the `cli` extra, which matters when the web
  process imports the same module.
- **Validation.** `__attrs_post_init__` runs the exposure selection (§5.2) at once. An
  unknown allowlisted key, a command without `include_writes`, or a sensitive operation fails
  at declaration, not at the first call. Because the declaration is usually built at module
  level, that failure surfaces while the loader imports the module, and exits 78 (§5.5).
- **Lazy runtime.** `runtime` is a factory, so `list` and `describe` never build a runtime,
  open a pool or need credentials. They read only the catalog.
- **Profile check.** The runtime a factory returns must have
  `deployment is DeploymentProfile.SERVERLESS`; otherwise `call` exits 78 naming the
  profile. Assembly then refuses any step *declared* `requires_long_running`, and the drain
  window is zero. The check is only as good as that declaration. A poller whose author did not
  declare it still starts, and §9 keeps that risk; nothing attests to the whole plan.

### 5.2 Exposure

This is MCP's policy, moved to core as `select_operations(catalog, *, include_writes,
operations, surface)`:
- With no allowlist, every read-only operation is exposed, plus every command when
  `include_writes=True`.
- An allowlist is checked like a grant. These are refused, not dropped:
  - an empty list;
  - an unknown key;
  - a command without `include_writes`.
- A sensitive operation is refused whichever way it was selected. This is the rule HTTP and
  MCP already apply, now in one place.
- `surface` only labels the error message.
- Agent tools keep their stricter rule, a mandatory allowlist with read-only as the default,
  by passing an allowlist every time.

The command name is the operation key verbatim, dots included: `orders.get`.

### 5.3 Identity

`OpsIdentity` is one of three values, and the declaration must name one.

| Value | Binds | Use |
|---|---|---|
| `ApiKeyIdentity(authn_spec, env="FORZE_API_KEY", tenancy_route=None)` | the principal the key authenticates, plus its resolved tenant | the default recommendation: per-operator or per-job keys, real attribution |
| `AnonymousIdentity()` | nothing | operations with no authn requirement; anything hooked with `AuthnRequired` fails `auth_required` |
| `StaticIdentity(authn=…, tenant=…, acknowledge_unverified_identity=True)` | a fixed identity, verified by nobody | single-tenant internal tools and break-glass inside the cluster, where shell access already equals database access |

**API key.**
- **Where the key comes from.** The secret comes from the named environment variable, or
  from `--api-key-file PATH`. There is no argv option, because argv is visible in `ps` and in
  shell history.
- **Verification.** The key goes through `ctx.authn.authn(authn_spec)
  .authenticate_with_api_key`, the same port HTTP and MCP use. Eligibility, expiry,
  revocation and delegation keys therefore behave identically on every surface.
- **When verification fails.**
  - An authentication failure exits 5, and the operation never runs.
  - An infrastructure or configuration failure keeps its own exit code: a deployment error is
    not a bad key.
- **Tenant.** It resolves from the principal through the tenancy resolver.
  `--tenant UUID` is a *requested* tenant, passed as `requested_tenant_id` and validated
  against membership. It is never trusted raw:
  - without a resolver, `--tenant` is refused (exit 2);
  - an issuer hint that conflicts with it raises `tenant_conflict`, through
    `coalesce_tenant_request_hints`;
  - a tenant outside the principal's membership raises the resolver's `tenant_mismatch`, an
    authentication kind (exit 5).

  A principal with no tenant binding, given no `--tenant`, runs with no tenant bound. That
  is what HTTP does with the same key (`resolvers.py:288-296`). An operation that requires a
  tenant then fails `TenantRequired` (`tenant_required`, an authentication kind, exit 5).

**Anonymous.** `--tenant` is refused (exit 2). There is no principal to validate it against.

**Static identity.**
- **The acknowledgement.** `acknowledge_unverified_identity` defaults to `False`, and the
  constructor raises `configuration` unless it is `True`. A default, rather than a required
  keyword, makes omission fail as `configuration` too; a required keyword would fail as a
  `TypeError` before validation runs. This is the
  RFC 0022 acknowledgement shape: the dangerous choice can still be spelled, but never by
  accident.
- **What it gives up.** The docs state the costs plainly:
  - no eligibility, expiry or revocation;
  - every operator shares one audit actor;
  - one principal-scoped idempotency namespace for every caller (RFC 0064). Two jobs that
    pass the same `--idempotency-key` share one claim. The second gets the first one's stored
    result when the input matches, and a `conflict` (exit 4) when it does not, because the
    store compares a payload hash (`hooks/idempotency/plans.py:160-161`). The docs tell jobs
    to prefix their keys with the job's name.
- **No tenant override.** `--tenant` is refused (exit 2), because there is nothing to
  validate it against.

**What is never offered.**
- Choosing a principal at run time: `--as`, `--principal`. That is impersonation, and it would
  turn shell access into "act as any user".
- A default identity of any kind.

### 5.4 The invocation envelope

| Option | Effect | Refused when |
|---|---|---|
| `--input JSON` / `@PATH` / `-` | the raw input, inline, from a file, or from stdin; validated into the DTO | the operation has no input type and input was given; the JSON does not parse; or the input is inline and the DTO declares a `SecretStr` or `SecretBytes` field, since argv is visible in `ps` and shell history (all exit 2, the last with a hint to use `-` or `@PATH`) |
| `--idempotency-key KEY` | `bind_idempotency(KEY)` | the entry's `supports_idempotency_key` is false (exit 2), so a script never believes a retry is safe when it is not (the reasoning in `forze_mcp/registration.py:157-160`) |
| `--timeout SECONDS` | `bind_deadline(SECONDS)`, which only ever tightens the plan's deadline. It bounds the operation, as HTTP's opt-in deadline does. Runtime startup, authentication and shutdown are bounded by the runtime's own step timeouts; a bound on the whole process belongs to the scheduler (`activeDeadlineSeconds`, `timeout(1)`) | not positive |
| `--grace SECONDS` | how long the first SIGINT or SIGTERM waits for the call to finish before cancelling it (§5.6); default 10 | negative |
| `--correlation-id UUID`, `--causation-id UUID` | carried into `InvocationMetadata` | malformed (exit 2). HTTP falls back silently on a bad header; a script passing a bad id has a bug |
| `--tenant UUID` | §5.3 | §5.3 |

**Input validation.**
- It goes through the shared `validate_operation_input(entry, raw, *, code="cli_invalid_input")`.
- Errors are sanitized with `sanitize_pydantic_errors`, so a secret typed into the wrong field
  is never echoed.
- Unknown keys are refused. A typo in a script must not be silently dropped.

**Metadata.** `execution_id` is always minted fresh with `uuid7`.

### 5.5 Output and exit codes

**One document on stdout.** After the declaration loads, `call` writes exactly one JSON
document to stdout and nothing else: the result when it exits 0, the error object otherwise.
A script reads stdout and the exit code, and never has to parse stderr.

- The result is encoded with `TypeAdapter(descriptor.output_type)` when the descriptor
  declares one, and with the pydantic/`JsonCodec` fallback agent tools use when it does not.
  `None` is written as `null`, and `--pretty` indents it.
- **The CLI keeps stdout clean itself.** An application's logging may well write to stdout,
  and the application module configures it, so the CLI cannot rely on it. For the duration
  of a call, the CLI duplicates the original stdout descriptor and points descriptor 1 at
  stderr (`os.dup2`). Logs, prints and a library's stray output all land on stderr. The one
  document is written to the saved descriptor.
- `list` and `describe` write text by default and JSON with `--json`.

**Stderr** carries everything else: the application's logs, and a server error logged through
the scrubbed logger as at the HTTP edge. Two failures happen before there is anything to put on
stdout:

| When | Output | Exit |
|---|---|---|
| Argument parsing, including a malformed `--correlation-id` or `--timeout` | Click's usage text on stderr | 2 |
| Loading the declaration: an import error, or a declaration that refuses itself | the traceback on stderr, without locals, as `forze dst` and `forze mock` print today. The operator needs it to fix their own module | 78 |
| Anything after that, through the end of the call | the error object on stdout | the table below |

The error object is built from `error_envelope` (masked, denial posture applied). A
non-`CoreException` gets `unhandled_error_envelope()` instead. A top-level handler around the
whole call guarantees one of the two:

```json
{"error": {"code": "…", "kind": "…", "detail": "…", "retryable": false, "context": {}},
 "execution_id": "…", "correlation_id": "…"}
```

`execution_id` and `correlation_id` are always present, because metadata is minted before the
first step that can fail (§5.6).

**An interrupt** is not an exception, so no envelope describes it. The CLI writes its own
object, with `kind: "interrupted"`, the one kind the CLI adds; `code: "interrupted"`;
`detail` naming the signal; and `retryable: false`, because an operation may have side effects
outside its transaction. Its exit code comes from the signal, 128 plus the signal number, not
from the kind table.

**Exit codes** are derived from the *envelope's* kind, not the raised one. A non-disclosing
denial posture (RFC 0059) collapses a denial about a covered resource type into not-found
(`envelope.py:153-154`), and that exits 3, just as HTTP answers 404. A denial about a type the
posture does not cover still exits 5.

| Exit | Meaning | From |
|---|---|---|
| 0 | success | |
| 1 | internal error | `internal` (including `commit_ambiguous`), any unhandled exception |
| 2 | usage | a parse error; an unknown or unexposed operation; malformed input JSON; an option the operation cannot honour |
| 3 | not found | `not_found` |
| 4 | rejected | `validation`, `domain`, `precondition`, `conflict` |
| 5 | denied | `authentication`, `authorization` |
| 6 | timed out | `timeout` |
| 69 | temporary failure; a write may have landed | a retryable kind, raised by a command called without an idempotency key |
| 75 | temporary failure; safe to retry as is | a retryable kind, raised by a query, or by a command called with an idempotency key |
| 78 | configuration | `configuration`; a declaration, runtime or profile refused before the operation ran |
| 130 / 143 | interrupted | SIGINT / SIGTERM (128 + signal number), when no commit had started (§5.6); the object's kind is the CLI-only `interrupted` |

**Why there are two retry codes.**
- Retry rules match on exit codes: Kubernetes `podFailurePolicy`, systemd's
  `RestartForceExitStatus=`, and queue runners that requeue on `EX_TEMPFAIL`. So an operator
  configures "retry on 75", and 75 must mean "repeating this cannot apply anything twice".
  That holds for a query. It also holds for a command whose idempotency key replays its
  stored result.
- A command without a key can fail `infrastructure` after its write landed. The classic case
  is a connection lost during `COMMIT`, which the driver reports as an infrastructure error.
  That gets 69 (`EX_UNAVAILABLE`), so it stays outside a "retry on 75" rule. The error object
  still says `retryable: true`, from the egress policy. The exit code adds what the policy
  cannot know: whether this call can be repeated safely.
- Retryability is read from the egress policy before the kind's row is consulted. If a policy
  changes, its kind moves between the retry codes and its row without anyone editing this
  table.

69, 75 and 78 come from `sysexits.h`, which schedulers and init systems already understand.

### 5.6 Running a call

1. Load the declaration. Its failures are the traceback case above.
2. Mint the invocation metadata, keeping a supplied `--correlation-id` and `--causation-id`.
   From here on, every failure writes the error object carrying both ids.
3. Check that the operation is exposed. Read and validate the input, and check the options
   against the catalog entry. None of this builds a runtime.
4. Build the runtime from the factory and check its profile.
5. Under `asyncio.run`, enter `runtime.scope()`, then:
   - resolve the identity;
   - bind identity, metadata, idempotency and deadline;
   - call `run_operation(registry, op, args, runtime.get_context())`.
6. Encode the result or the error, then leave the scope. Shutdown runs whether the call
   succeeded or not.

**Signals.** `asyncio.run` handles only SIGINT, and SIGTERM's default action kills the
process with no shutdown at all. The command therefore installs handlers for both with
`loop.add_signal_handler`, and follows RFC 0074's two-signal shape:

1. **The first signal** does not cancel anything. It waits up to `--grace` seconds for the
   call to finish, because cancelling a commit leaves its outcome unknown. A call that
   finishes in time exits with its own code. That is still correct after an interrupt:
   finishing is what the grace was for.
2. **A second signal, or the grace running out,** cancels the operation. The CLI reads
   `commit_started()` as soon as the cancellation reaches its frame, before it leaves the
   scope: a lifecycle shutdown hook that runs an operation would clear the flag. Then the
   scope shuts down, and the CLI reports one of two outcomes:
   - the commit point had been reached: `internal` / `commit_ambiguous`, exit 1;
   - it had not: the `interrupted` object (§5.5), exit 130 or 143.

The deadline path cannot do this for us. The flag's own docstring says a deadline *or a
cancellation* after the commit point is ambiguous
(`execution/context/commit_state.py:16-28`). But the check in `invoke.py:104-122` catches
only `TimeoutError`, and cancellation raises `CancelledError`, which passes it by. Reading
the flag at the CLI boundary keeps the CLI correct without changing how cancellation behaves
everywhere else. Whether the other boundaries need the same check is recorded in §12.

**As built (P3).** The other boundaries record a late cancel; none of them converts it.
Execution log, Unit "RFC 0077 P3", entries D-095 to D-097.

- **The record lives in one place.** `ResolvedOperation.__call__` is the top-level boundary
  every surface reaches: `run_operation` and `invoke_at_ingress` go through it, and Socket.IO
  and Inngest call it directly. When a cancel reaches it with the commit flag set, it logs a
  `commit_ambiguous` warning and adds a `forze.commit_ambiguous` event to the active span.
  Then it re-raises the cancel. A nested operation records nothing; its top-level caller
  records once. The P2 contract stands: the flag survives the cancel, and the CLI converts.
- **The conversion stays with boundaries that own the outcome.**

  | Boundary | Late cancel comes from | Decision |
  | --- | --- | --- |
  | `forze ops call` | a second signal or the grace running out | converts (P2): the CLI owns its process and its stdout |
  | FastAPI routes, MCP, Socket.IO, WS commands | a client that left, or a server shutdown | record only: no one is left to read an error, and shutdown needs the cancel |
  | Durable runner, Inngest, app-written Temporal activities | a drain, an operator cancel, `max_run_duration`, the engine's own cancel | record only: each reads the raw cancel to name its cause (D-096) |
  | Saga executors | any of the above, per step | already convert, at their own boundary: they own the journal |
  | Inbox consumer, outbox relay | shutdown | unchanged: no operation at the top level; inbox dedup covers a redelivery |

- **Audit is unchanged.** `AuditOutcome` has no ambiguous value. A command's audit row is
  written inside its transaction, so it commits with the operation.
- **A read-only transaction marks the commit point unless its manager proves read-only**
  (D-097). Every root used to mark it, so a query cancelled or timed out after its read
  reported `commit_ambiguous`. A manager implementing `ReadOnlyAware` is now asked, just
  before the commit, whether the backend refused writes in the transaction it actually
  opened. Postgres answers yes only after a real `BEGIN … READ ONLY`, and the mock's
  `journal` and `strict` managers only for a root they opened read-only. A root nested in a
  transaction opened outside forze is a savepoint and answers no. Postgres also answers no
  once a `detached()` statement, or another client's transaction or autocommit statement,
  could have committed beside the root. The kernel asks only while the root is the only route entered, because
  a nested scope on another route opens a transaction of its own. A root that answers yes
  marks nothing, so its query exits 6 (`deadline_exceeded`) or 130 or 143 (interrupted).
  Mongo, Firestore and Neo4j accept `read_only` without enforcing it, and forze's write
  guard covers a `QUERY` operation, not the transaction. They do not implement the
  protocol, so their read-only roots still mark the point and still report
  `commit_ambiguous`.

### 5.7 Shared core (P1): `forze.application.execution.ingress`

`ingress` sits in the execution layer because it needs `ExecutionContext`. Integrations sit
below that layer and cannot import it.

| Function | Replaces |
|---|---|
| `select_operations(catalog, *, include_writes, operations, surface)` | the body of `forze_mcp.projection.exposed_operations` |
| `refuse_sensitive(catalog, ops, *, surface)` | the sensitive refusal in `register_tools` |
| `validate_operation_input(descriptor, raw, *, code, forbid_unknown)`: both keywords required, no defaults | the body of `forze_mcp.dispatch.build_args`, which passes `forbid_unknown=False` to keep its current behaviour; the CLI passes `True`; agent tools' `_validated_args` later |
| `invoke_at_ingress(registry, ctx, op, args, *, authn, tenant, metadata, idempotency_key=None, deadline=None)` | `forze_mcp.dispatch.invoke_operation`'s bind-and-run |
| `mint_invocation_metadata(*, correlation_id=None, causation_id=None)` | the fresh-`uuid7` blocks in MCP and FastAPI |
| `IngressIdentityResolver` (protocol), `StaticIdentityResolver` | `forze_mcp.identity.MCPIdentityResolver` and its static resolver, which have no fastmcp dependency today |
| `authenticate_api_key(ctx, spec, raw)`, `resolve_principal_tenant(ctx, result, route, requested)` | `forze_mcp.auth._split_api_key` and `_resolve_tenant`; FastAPI's copies later |

- **MCP keeps its names and signatures.**
  - The two classes, `MCPIdentityResolver` and `StaticIdentityResolver`, are re-exported
    as the ingress ones; their shapes are identical.
  - Every function keeps its current signature as a thin wrapper over `ingress`:
    `exposed_operations`, `build_args`, `invoke_operation`, and the private auth helpers. For
    example, `build_args(descriptor, arguments)` calls `validate_operation_input` with
    `code="mcp_invalid_arguments"` and `forbid_unknown=False`.
  - `exposed_operations` keeps leaving the sensitive refusal to `register_tools`, so a direct
    caller sees no change.
  - P1 therefore changes no import, no signature and no behaviour, and `forze_mcp`'s existing
    suite is the regression net.
- **Error codes stay per surface:** `code` and `surface` are parameters, so clients see the
  same codes as before.
- **Later moves.** FastAPI, Socket.IO and the WS route move onto `ingress` in later changes,
  each against its own tests. That is recorded in §12 and not scheduled here.
- **As built (P1).** Four departures from the plan above:
  - `validate_operation_input` also takes `summary`, the rejection's message (default
    `"Invalid input"`), so MCP keeps `Invalid tool arguments`. With `forbid_unknown`, an
    unknown name is refused under the same `code`, with `details={"unknown": [...]}`.
  - The accepted-names check moved from agent tools into `ingress` as
    `accepted_input_names`, and agent tools call it, so there is one copy before agent tools
    move onto `validate_operation_input`.
  - `api_key_credentials(raw)` is public, for FastAPI's later move. MCP's private
    `_split_api_key` and `_resolve_tenant` were deleted instead of wrapped: nothing called
    them once the verifier called `ingress`.
  - `resolve_principal_tenant` refuses a `requested` tenant that neither a resolver nor the
    credential vouches for. With no resolver, that is `precondition` with code
    `tenant_request_unverifiable`; when the resolver finds no tenant for the principal, it is
    `authentication` with code `tenant_mismatch`. The call never runs tenant-less instead. A
    `requested` tenant equal to the key's own tenant is bound, as over HTTP. MCP never passes
    one.
- **As built (P3).** FastAPI and the WS route moved each step that agrees with `ingress` on
  every input they send. Socket.IO moved its metadata minting only. The execution-log unit
  "RFC 0077 P3 — surfaces onto ingress" holds the comparison and a decision for each step.
  - FastAPI's invocation middleware mints through `mint_invocation_metadata` and keeps its
    advisory header parsing. Its API-key header goes through `authenticate_api_key` after
    its own blank and `required` check. Tenant resolution, route validation, the sensitive
    refusal and the middleware binding stay, because each differs in a way the move would
    make visible.
  - The WS route's command frames run through `invoke_at_ingress`. That is a behaviour
    change: a frame now gets fresh invocation metadata, where before it had none.
  - Socket.IO's events mint metadata through `mint_invocation_metadata` and bind it around
    each run, the same behaviour change. Review found they bound no tenant, and they now
    bind their connection's tenant as WS frames do. The adapter still dispatches through a
    handler resolver, not a registry, so the bind-and-run did not move. Its payload
    validation and identity ladder are shared with the WS route, below the execution layer.

### 5.8 Commands

```text
forze ops list      [--app REF] [--json]
forze ops describe  [--app REF] OP [--json]
forze ops call      [--app REF] OP [--input JSON|@PATH|-] [--tenant UUID]
                    [--idempotency-key KEY] [--timeout SECONDS] [--grace SECONDS]
                    [--correlation-id UUID] [--causation-id UUID]
                    [--api-key-file PATH] [--pretty]
```

- **Finding the declaration.** `--app` falls back to `FORZE_OPS_APP`, so a project sets it
  once.
- **Embedding.** `build_ops_cli(app, *, name="ops") -> typer.Typer` returns the same group
  with the declaration bound. `myapp ops call …` then needs no `--app`. This is the same
  split as MCP's `register_tools` and `build_mcp_server`.
- **What `describe` shows:**
  - title, description and kind;
  - declared authn, permissions, idempotency support and deadline;
  - the input and output JSON Schemas;
  - the query discovery text from `describe_query_discovery`, the helper MCP and agent tools
    already share.
- **Crash output.** Typer reads `pretty_exceptions_show_locals` from the app that was invoked,
  not from a mounted group (`Typer.__call__`). `forze ops` inherits the root's `False`. An
  application that mounts the group in its own CLI controls this through its own root, and
  `build_ops_cli` cannot set it for them. The docs therefore say to set it, and a test asserts
  the warning is in the docstring.
- **As built (P2).** Departures from §5.1–§5.8:
  - **The commit flag survives a cancellation.** §5.6 assumed the CLI could read
    `commit_started()` where the cancellation reaches its frame. `run_operation`'s top-level
    boundary cleared the flag in its `finally` first. It now leaves the flag set when the call
    is cancelled; every reader resets it on entry. A cancel that lands inside a transaction's
    commit was already converted to `commit_ambiguous` by the transaction scope. The flag
    covers a cancel after the scope has gone, or after an earlier root transaction committed.
  - **Signals cover the whole call.** The first signal arms the grace timer in any phase. A
    signal during startup skips the operation and reports `interrupted`, and a failed shutdown
    after the call keeps the call's outcome and logs the failure to stderr.
  - **Error codes** carry a `cli_` prefix. Usage refusals are `validation` errors that exit 2;
    the reference page lists them. A `--tenant` nothing can validate keeps the ingress code
    `tenant_request_unverifiable` and exits 2. A `--tenant` equal to the key's own tenant is
    bound without a resolver, as over HTTP.
  - **The exit-code table** maps the non-retryable kinds only, since retryability is read
    first. A test fails when a non-retryable kind has no row.
  - **`--app`** also accepts a zero-argument callable returning an `OpsApp`, as `forze mock`
    does. Naming none exits 2.
  - **Skills.** No reference file was added. A section with an import block went into
    `fastapi-generated-routes`, the route RFC 0041 §6.1 sets for content without a file, and
    `forze_cli` moved from D3 to D1.

### 5.9 Alternatives considered

- **A new `forze_ops` package with its own extra.** Rejected. The console script, the loader,
  the extra gating and the no-locals crash handling already live in `forze_cli`, and its
  docstring reserves `ops`. A second package needs a second console script, or a plugin
  mechanism for one group.
- **Importing `forze_mcp`'s helpers.** Rejected. Importing `forze_mcp` requires the `mcp`
  extra, so a terminal command that never speaks MCP would pull in fastmcp.
- **Building on the agent-tools bridge (RFC 0023).** Rejected, for three reasons:
  - `forze_kits` admits only `forze_fastapi` as an importer;
  - its dispatch expects a context someone else already bound;
  - its mandatory allowlist and read-only default fit an agent's palette, not an operator.
- **Copying the boundary sequence into `forze_cli`.** The smallest diff, and rejected for
  exactly that reason: it would be the fourth copy (§2), and the copies already disagree.
- **A static identity as the default.** It is the simplest choice for single-tenant tools,
  and it is what an ad-hoc script does today. Rejected as a default because it gives up
  verification, revocation and attribution without saying so. It stays available behind the
  acknowledgement.
- **Access tokens instead of API keys.** Rejected. Forze's access tokens are tied to a live
  session row and refused once the session is revoked or rotated
  (`forze_identity/authn/verifiers/forze_jwt_token.py:39-42`), so a cron job cannot hold one.
- **Generated per-operation commands with typed flags.** They would be friendlier for
  humans. Rejected for the first version: nested, union and list DTOs have no clean flag
  mapping, and a partial mapping would make the contract depend on each DTO's shape. The
  flat-scalar case is demand-gated (§12).
- **One exit code for every failure, or HTTP statuses as exit codes.** With one code, a
  script must parse the error object to decide anything. HTTP statuses do not fit in an exit code, which
  stops at 255, and the range above 128 already means "killed by a signal".
- **A remote mode.** See §8.

## 6. Tests

The battery lives under `tests/unit/test_forze_cli/test_ops_*.py` and uses Typer's
`CliRunner` over a module-level sample app. That is the existing `test_cli.py` pattern.

1. **Parity with MCP.** The same operation through the CLI and through MCP dispatch gives an
   equal result and an equal denial code. It also masks a validation error equally.
2. **Exposure.**
   - Only queries appear by default.
   - A command is absent without `include_writes`.
   - An allowlist refuses an empty list, an unknown key, and a command without
     `include_writes`.
   - A sensitive operation is refused, and the refusal comes at declaration.
3. **Input.**
   - Inline, `@file` and stdin all work.
   - Malformed JSON exits 2.
   - Inline input for a DTO with a `SecretStr` field exits 2, and the same input through
     stdin succeeds.
   - An unknown key exits 4.
   - A value typed into the wrong field never appears in the error output.
4. **Identity.**
   - An environment key binds its principal.
   - An invalid key exits 5, and the handler never runs.
   - No option accepts a key on argv.
   - A static identity with the acknowledgement omitted, or `False`, raises `configuration`.
   - Anonymous on an `AuthnRequired` operation exits 5.
   - `--tenant` is refused with exit 2 without a resolver, with a static identity, and with
     an anonymous one.
   - A tenant outside membership exits 5.
   - A principal with no tenant, on a `TenantRequired` operation, exits 5.
5. **Envelope.**
   - The correlation id reaches `InvocationMetadata`, and the error object echoes it.
   - An idempotency key on an operation without support exits 2.
   - A repeated key returns the stored result. The same key with different input exits 4.
   - `--timeout` exits 6.
6. **Exit-code table.**
   - The test iterates `ExceptionKind`, so a new kind fails until it is mapped.
   - A collapsed denial exits 3, and a denial the posture does not cover exits 5.
   - A retryable kind exits 75 from a query and from a keyed command, and 69 from a command
     without a key.
7. **Output shape.**
   - A parse error prints Click's usage text on stderr and exits 2.
   - An import error in the application module prints a traceback without locals on stderr
     and exits 78.
   - Every failure after loading writes exactly one JSON object to stdout, an unhandled
     exception included. The object carries `execution_id` and `correlation_id`, even when the
     input or the API key is what failed.
   - An application that logs and prints to stdout during a call still leaves exactly one
     JSON document there, and its output appears on stderr.
   - An interrupt before the commit point writes the `interrupted` object.
8. **Runtime and signals.**
   - A runtime that is not `SERVERLESS` exits 78.
   - Shutdown hooks run after success and after failure.
   - A first SIGINT or SIGTERM lets a call that finishes within `--grace` exit with its own
     code.
   - A second signal, or an expired grace, exits 130 or 143 when the cancel lands before
     the commit point. When it lands after the commit point, it exits 1 with
     `commit_ambiguous`. Shutdown hooks run in both cases.
   - `list` and `describe` never call the runtime factory.
9. **Secrets.** A crash under `forze ops` prints no locals.
10. **Extras.** `forze ops --help` renders without the operation's backends, and the
    declaration imports without Typer.
11. **P1 regression.** The existing `forze_mcp` suite passes unchanged after the move. New
    unit tests cover each `ingress` function directly.

## 7. Docs

- A how-to page: `pages/docs/running-in-prod/operations-cli.md`. It covers declaring
  `OpsApp`, building a `SERVERLESS` runtime, choosing an identity, Kubernetes Job and cron
  examples, and what a static identity gives up.
- A reference page: `pages/docs/reference/ops-cli.md`. It holds the output contract, the
  error object, the interrupt object and the exit-code table, and links to [errors](../pages/docs/reference/errors.md).
- `integrations/index.md`: extend the `cli` extra's line.
- `app.py`: rewrite the root help, which today speaks only of DST.
- A test-backed example under `examples/`, exercised from `tests/unit/test_examples/`.
- The skills census: `forze_cli` leaves D3 because an application now reaches it. It gets a
  doctrine and a reference file under `skills/forze-skills/`, per RFC 0042.

## 8. Out of scope

- **A remote mode**, where the CLI calls a deployed service over HTTP or MCP. It would avoid
  putting infrastructure credentials where the command runs. But it needs a deployed server,
  inherits MCP's read-only default, and is a client product rather than a boundary. Record it
  if operators ask; the in-process mode covers Jobs, cron and `kubectl exec`, which is the
  demand today.
- **Generated typed flags per operation.** Nested, union and list DTOs map badly onto flags.
  §12 records the flat-scalar case as demand-gated, using the flat signature MCP already
  synthesises.
- **JSONL batch mode** (many inputs, one scope), for backfills. Demand-gated in §12.
- **A field naming the calling surface** in `InvocationMetadata`, the log fields and
  `AuditEntry`. It changes RFC 0060's entry for every surface, so it is worth its own RFC.
  This RFC does not wait for it (§10).
- **Confirmation prompts for commands.** Writes are gated at declaration
  (`include_writes`) and by permission. A prompt adds nothing for cron and little for humans.

## 9. Risks

- **Credentials where the command runs.** In-process means the operator's environment holds
  the application's infrastructure secrets. The docs recommend running inside the cluster, as
  a Job or through `kubectl exec`, rather than on a laptop.
- **Undeclared long-running steps still start.** The `SERVERLESS` check trusts the
  `requires_long_running` marker. A poller whose author did not declare it still starts
  under a command. The docs say to build the CLI runtime from only the deps and steps it
  needs.
- **Unguarded operations become reachable.** The tenancy admin kit ships unguarded by design
  (`forze_kits/aggregates/tenancy_admin/operations.py:5`). Mitigations:
  - `include_writes` defaults off;
  - the allowlist is the recommended shape for a command-capable declaration;
  - the docs name this case.
- **An embedding CLI that prints locals.** An application can mount `build_ops_cli` under a
  root Typer that shows locals: set explicitly, or by default on the older Typer releases the
  `cli` extra still admits (`typer>=0.12.0`; 0.27.3 defaults to `False`). That application
  prints secrets on a crash. Typer gives the group no way to prevent it (§5.8), so the defence
  is the docstring and the docs.
- **Import side effects.** Loading `module:attr` runs the application module's top level.
  Keeping it cheap is the application's responsibility, as it already is for `forze dst` and
  `forze mock`.
- **The contract becomes semver surface.** Exit codes, the error object and the stdout shape
  are breaking to change once released. That is why they are fixed here, before code.
- **Startup per call.** Each `call` opens and closes its runtime. That is fine for admin work
  and wrong for loops, which the batch mode in §12 would answer.

## 10. Unresolved questions

None open. The three this draft asked were answered by the maintainer on 2026-10-22, and each
answer took the recorded lean.

1. **Reuse `SERVERLESS`, or add a `ONE_SHOT` profile?** The checks and defaults are
   identical, but "serverless" reads oddly on a Kubernetes Job. **Answer: reuse it.** P2 adds
   a sentence to the docstring: it also covers "a process that hosts no long-running step and
   has no drain window". A new member stays cheap to add later, while a second profile with
   identical semantics would not be cheap to remove. (Decision 14.)
2. **Should P2 wait for a surface field in audit and logs?** Without one, an audit row
   written by a cron job looks like one written over HTTP. **Answer: no.** With an API-key
   identity the actor already names the job or the operator, and the correlation id ties the
   row to the command's logs. The field changes RFC 0060's locked entry for every surface, so
   it is its own RFC, not a prerequisite. A static identity loses this attribution, and §5.3
   says so. (Decision 16.)
3. **Should a static identity also raise a production-posture finding?** The acknowledgement
   already makes it deliberate. A posture finding would refuse it in production unless the
   posture opts out. **Answer: no.** Break-glass in the production cluster is the main
   legitimate use, and refusing it there pushes operators back to raw scripts.
   (Decision 17.)

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `ASSUMED` | A `forze ops` group in `forze_cli` under the existing `cli` extra, not a new package. |
| 2 | `LOCKED` | Every call goes through `run_operation` on the runtime's scope context, after validation and identity binding. There is no direct handler path. |
| 3 | `ASSUMED` | The boundary sequence moves to `forze.application.execution.ingress` before the CLI is built, and `forze_mcp` moves onto it in the same phase with no behaviour change. |
| 4 | `ASSUMED` | The identity choice is a required declaration field with no default: a verified API key, anonymous, or a static identity that is refused unless its acknowledgement is `True`. |
| 5 | `LOCKED` | No run-time choice of principal (`--as`), and no API key on argv. |
| 6 | `LOCKED` | `--tenant` is a requested tenant validated by the resolver, never trusted raw, and refused wherever there is nothing to validate it against. |
| 7 | `ASSUMED` | Exposure is MCP's policy: queries by default, `include_writes` opt-in, an optional allowlist checked like a grant, sensitive operations refused. |
| 8 | `ASSUMED` | After loading, stdout carries exactly one JSON document, the result or the masked error object, and the CLI redirects everything else the application writes to stderr. Metadata is minted before the first fallible step. Exit codes follow §5.5 and are derived from the envelope, except an interrupt's, which comes from the signal. |
| 9 | `LOCKED` | Exit 75 means safe to repeat as is. A retryable failure of a command called without an idempotency key exits 69 instead. |
| 10 | `LOCKED` | An interrupted call is never reported as safe when its commit may have landed. The first signal waits up to `--grace`; a cancel after the commit point reports `commit_ambiguous`. |
| 11 | `ASSUMED` | A command runs only on a `SERVERLESS` runtime, the profile that hosts no long-running step. The runtime is built lazily, so `list` and `describe` never build one. |
| 12 | `LOCKED` | An idempotency key is refused for an operation that does not support one. |
| 13 | `ASSUMED` | Inline `--input` is refused for a DTO declaring `SecretStr` or `SecretBytes` fields. |
| 14 | `LOCKED` | One-shot processes reuse `DeploymentProfile.SERVERLESS`, and no new profile is added. Its docstring is extended to cover them (§10 Q1). |
| 15 | `ASSUMED` | No typed flags, JSONL batch, remote mode, or surface field in this RFC. |
| 16 | `LOCKED` | P2 does not wait for a surface field in `InvocationMetadata`, the log fields or `AuditEntry`. That field is its own RFC (§10 Q2). |
| 17 | `LOCKED` | A static identity raises no production-posture finding. The required acknowledgement is the whole gate (§10 Q3). |

## 12. Phasing

- **P1: shared ingress core.** Add `forze.application.execution.ingress` (§5.7), move
  `forze_mcp` onto it with aliases kept, and add unit tests per function. No behaviour
  change. No changelog entry either, because nothing user-facing changes.
- **P2: `forze ops`.** The declaration, the three commands, the identities, the envelope,
  the exit-code contract, `build_ops_cli`, the test battery (§6), the docs and example (§7),
  the skills census move, and the `SERVERLESS` docstring extension (decision 14).
  Changelog: Added.
- **P3: demand-gated, each on a named consumer:**
  - typed flags for flat scalar DTOs;
  - JSONL batch in one scope;
  - shell completion of operation names;
  - the surface field (its own RFC);
  - moving FastAPI, Socket.IO and the WS route onto `ingress`. FastAPI and the WS route
    moved where a step agrees (§5.7, "As built (P3)"). Socket.IO mints its metadata, and its
    bind-and-run waits for a registry-backed adapter;
  - ~~deciding whether a cancellation after the commit point should report `commit_ambiguous`
    at every boundary, not only the CLI (§5.6).~~ Done on 2026-10-27: every boundary
    records it and only the CLI converts it (§5.6, "As built (P3)").
