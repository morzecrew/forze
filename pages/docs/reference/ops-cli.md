---
title: Operations CLI
icon: lucide/terminal
summary: The forze ops commands, options, output contract and exit codes
---

The how-to is [Operations CLI](../running-in-prod/operations-cli.md); this page is the
contract scripts rely on. The exit codes, the error object and the stdout shape are stable
across releases.

## Commands

```text
forze ops list      [--app REF] [--json]
forze ops describe  [--app REF] OP [--json]
forze ops call      [--app REF] OP [--input JSON|@PATH|-] [--tenant UUID]
                    [--idempotency-key KEY] [--timeout SECONDS] [--grace SECONDS]
                    [--correlation-id UUID] [--causation-id UUID]
                    [--api-key-file PATH] [--pretty]
```

`OP` is the operation key, dots included: `orders.get`. `--app` names the `OpsApp` as
`module:attribute`, or a zero-argument callable returning one, and falls back to
`FORZE_OPS_APP`. A group from `build_ops_cli(app)` has the declaration bound and no `--app`.

`list` and `describe` never build the runtime. They write text, or JSON with `--json`.
`describe` shows the title and description, the kind, the declared authn requirement and
permissions, idempotency support, the deadline, the query surface, and the input and output
JSON Schemas.

## `call` options

| Option | Effect | Refused when |
|---|---|---|
| `--input JSON` / `@PATH` / `-` | the input object, inline, from a file, or from stdin; validated into the operation's input DTO, unknown keys refused | the operation takes no input; the JSON does not parse (`NaN` and `Infinity` included) or is not an object; the file cannot be read; the input is inline and the DTO declares a `SecretStr` or `SecretBytes` field |
| `--tenant UUID` | a requested tenant, validated against the principal's membership, or matching the key's own tenant | the identity is not an API key; nothing can validate it |
| `--idempotency-key KEY` | binds the key: a repeat with the same input replays the stored result | the operation does not support one |
| `--timeout SECONDS` | bounds the operation; only ever tightens its declared deadline | not a positive, finite number |
| `--grace SECONDS` | how long a first SIGINT or SIGTERM waits for the call to finish; default 10 | negative, or not finite |
| `--correlation-id UUID`, `--causation-id UUID` | carried into the invocation metadata | malformed |
| `--api-key-file PATH` | reads the API key from a file instead of the environment | the identity is not an API key; the file cannot be read |
| `--pretty` | indents the result | |

A malformed `--timeout`, `--grace`, `--tenant` or id is a parse error: Click's usage text on
stderr, exit 2, nothing on stdout. Every other refusal is an error object.

## Output

After the declaration loads, `call` writes exactly one JSON document to stdout, followed by a
newline. For the duration of the call, file descriptor 1 points at stderr, so whatever the
application logs or prints lands there.

**The result** is encoded with the operation's declared output type. An operation that returns
nothing writes `null`.

**The error object** is the masked [error envelope](errors.md): a server-side error carries
the generic detail, and a denial about a resource type a non-disclosing posture covers reads
as not found.

```json
{"error": {"code": "core.authentication", "kind": "authentication", "detail": "Invalid API key",
           "retryable": false, "context": {}},
 "execution_id": "…", "correlation_id": "…"}
```

`execution_id` and `correlation_id` are always present, whatever failed: they are minted before
the first step that can fail.

**An interrupt** before any commit started writes the one kind the CLI adds:
`"code": "interrupted"`, `"kind": "interrupted"`, a `detail` naming the signal, and
`"retryable": false`, since an operation may have side effects outside its transaction.

Two failures write nothing to stdout:

| When | Output | Exit |
|---|---|---|
| Parsing the arguments | Click's usage text on stderr | 2 |
| Loading the declaration: no `--app`, an import error, a declaration that refuses itself, an object that is not an `OpsApp` | a message, or the traceback without locals, on stderr | 2 when none is named, else 78 |

## Exit codes

The code is derived from the error envelope's kind, after the denial posture applies.
Retryability is read from the kind's egress policy first.

| Exit | Meaning | From |
|---|---|---|
| 0 | success | |
| 1 | internal error | `internal`, including `commit_ambiguous`; any unhandled exception |
| 2 | usage | a parse error; an unknown or unexposed operation; an input or option the operation cannot take |
| 3 | not found | `not_found`, including a collapsed denial |
| 4 | rejected | `validation`, `domain`, `precondition`, `conflict` |
| 5 | denied | `authentication`, `authorization` |
| 6 | timed out | `timeout` |
| 69 | temporary failure; a write may have landed | a retryable kind, from a command called without an idempotency key |
| 75 | temporary failure; safe to retry as is | a retryable kind, from a query or from a command called with an idempotency key |
| 78 | configuration | `configuration`; the declaration or the runtime refused before the operation ran, a runtime that is not `SERVERLESS` included |
| 130 / 143 | interrupted | SIGINT / SIGTERM, cancelled before any commit started |

69, 75 and 78 are `EX_UNAVAILABLE`, `EX_TEMPFAIL` and `EX_CONFIG` from `sysexits.h`. The
retryable kinds are `concurrency`, `throttled` and `infrastructure`.

## Error codes the CLI adds

| Code | Kind | Exit | Raised when |
|---|---|---|---|
| `cli_unknown_operation` | `validation` | 2 | the operation is unknown, or not exposed |
| `cli_input_not_accepted` | `validation` | 2 | `--input` is given to an operation that takes none |
| `cli_input_unreadable` | `validation` | 2 | the `@PATH` file cannot be read |
| `cli_malformed_input` | `validation` | 2 | the input is not JSON, or not an object |
| `cli_inline_secret` | `validation` | 2 | the input declares a secret and was given inline |
| `cli_idempotency_unsupported` | `validation` | 2 | `--idempotency-key` on an operation that does not support one |
| `cli_tenant_unverifiable` | `validation` | 2 | `--tenant` with an anonymous or static identity |
| `tenant_request_unverifiable` | `precondition` | 2 | `--tenant` that neither a resolver nor the key's own tenant can validate |
| `cli_api_key_file_unused` | `validation` | 2 | `--api-key-file` without an API-key identity |
| `cli_api_key_unreadable` | `validation` | 2 | the `--api-key-file` file cannot be read |
| `cli_invalid_input` | `validation` | 4 | the input does not validate, or names an unknown key |
| `cli_api_key_missing` | `authentication` | 5 | the key's environment variable is unset or blank, and no file is given |
| `cli_runtime_not_serverless` | `configuration` | 78 | the runtime factory returned a runtime that is not `SERVERLESS` |
| `cli_unverified_identity_unacknowledged` | `configuration` | — | a `StaticIdentity` built without `acknowledge_unverified_identity=True`; raised while the module imports |

## Environment

| Variable | Read by | Default |
|---|---|---|
| `FORZE_OPS_APP` | `--app` | none |
| `FORZE_API_KEY` | `ApiKeyIdentity(env=…)` | the variable an API-key identity reads |
