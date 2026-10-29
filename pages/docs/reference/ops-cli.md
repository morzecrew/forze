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
forze ops call      [--app REF] OP
                    [--input JSON|@PATH|- | --FIELD VALUE ... | --batch @PATH|-]
                    [--tenant UUID] [--idempotency-key KEY]
                    [--timeout SECONDS] [--grace SECONDS]
                    [--correlation-id UUID] [--causation-id UUID]
                    [--api-key-file PATH] [--pretty]
```

`OP` is the operation key, dots included: `orders.get`. `--app` names the `OpsApp` as
`module:attribute`, or a zero-argument callable returning one, and falls back to
`FORZE_OPS_APP`. A group from `build_ops_cli(app)` has the declaration bound and no `--app`.

`OP` completes in the shell after `describe` and `call` once completion is installed with
`forze --install-completion`. Completion loads the declaration from `--app` or
`FORZE_OPS_APP` as `list` does, never builds the runtime, and offers nothing when the
declaration cannot load. A group from `build_ops_cli` completes from its own declaration,
installed by your root CLI's `--install-completion`. Every TAB imports the application
module, so its import side effects run in the operator's shell.

`list` and `describe` never build the runtime. They write text, or JSON with `--json`.
`describe` shows the title and description, the kind, the declared authn requirement and
permissions, idempotency support, the deadline, the typed flags, the query surface, and the
input and output JSON Schemas.

## `call` options

| Option | Effect | Refused when |
|---|---|---|
| `--input JSON` / `@PATH` / `-` | the input object, inline, from a file, or from stdin; validated into the operation's input DTO, unknown keys refused | the operation takes no input; the JSON does not parse (`NaN` and `Infinity` included) or is not an object; the file cannot be read; the input is inline and the DTO declares a `SecretStr` or `SecretBytes` field |
| `--batch @PATH` / `-` | one call per line of a JSONL file or of stdin, in one runtime; see [Batches](#batches) | the value is neither `@PATH` nor `-`; combined with `--input`, typed flags, `--idempotency-key` or `--pretty` |
| `--tenant UUID` | a requested tenant, validated against the principal's membership, or matching the key's own tenant | the identity is not an API key; nothing can validate it |
| `--idempotency-key KEY` | binds the key: a repeat with the same input replays the stored result | the operation does not support one |
| `--timeout SECONDS` | bounds the operation; only ever tightens its declared deadline | not a positive, finite number |
| `--grace SECONDS` | how long a first SIGINT or SIGTERM waits for the call to finish; default 10 | negative, or not finite |
| `--correlation-id UUID`, `--causation-id UUID` | carried into the invocation metadata | malformed |
| `--api-key-file PATH` | reads the API key from a file instead of the environment | the identity is not an API key; the file cannot be read |
| `--pretty` | indents the result | |

A malformed `--timeout`, `--grace`, `--tenant`, `--batch` or id is a parse error: Click's usage text on
stderr, exit 2, nothing on stdout. Every other refusal is an error object, an option `call`
does not have included: it is read as a typed flag.

## Typed flags

An operation whose input is flat scalars also takes that input as flags after `OP`, instead of
`--input`:

```console
$ forze ops call notes.create --title "Quarterly report"
```

| Field type | Flag |
|---|---|
| `str`, `int`, `float`, `Decimal`, `UUID`, `date`, `datetime`, an `Enum` of strings, an `IntEnum`, a `Literal` of strings; optional or constrained ones too | `--field-name VALUE`, or `--field-name=VALUE` |
| `bool` | `--field-name` sets it, `--no-field-name` clears it |

- **Spelling.** A field's flag is its name with `-` for `_`. An alias never is.
- **Validation.** The flags build the object `--input` would carry, and it is validated the same
  way: each value is the string you typed, coerced by the DTO; an omitted flag takes the
  field's default; a missing required field or a bad value exits 4 and is never echoed.
- **A value that looks like a flag.** `--title --pretty` gives `--pretty` to `call`, and
  `--title --draft` is refused: the token after a flag that needs a value cannot start with
  `--`. Write `--title=--pretty`.
- **On argv.** Every value is on the command line, where `ps` and shell history show it. A
  secret field is refused; pass any other sensitive value with `--input @PATH`.
- **All or none.** An input takes no flags when one of its fields is anything else (a list, a
  model, a union), when it declares a `SecretStr` or `SecretBytes` field, or when a flag would
  be spelled like a `call` option (`--timeout`, `--input`, …) or like another field's. A `call`
  option means the same for every operation. `describe` lists the flags, or says why there are
  none.
- **Exclusive.** Typed flags and `--input` cannot be combined.

## Batches

`--batch @PATH` or `--batch -` calls the operation once per line of a JSONL file, or of stdin,
inside one runtime: one startup and one shutdown for the whole batch. Each line is an input
object, as `--input` would carry; blank lines are skipped.

```console
$ forze ops call notes.create --batch @notes.jsonl
{"line": 1, "result": {"id": "…", "title": "a", …}, "execution_id": "…", "correlation_id": "…"}
{"line": 2, "result": {"id": "…", "title": "b", …}, "execution_id": "…", "correlation_id": "…"}
```

- **Validated first.** Every line is read and validated before the runtime is built. A line
  that does not parse or validate is the only line written, and nothing runs. The whole input
  is held in memory, so split a file too large for that.
- **One line out per call.** Stdout holds one JSON document per line that ran, in order, each
  written as its call finishes: `{"line": N, "result": …}` or the error object with
  `"line": N`. Each carries its own `execution_id`; all share one `correlation_id`, the
  `--correlation-id` given or one minted for the batch. Each line is a governed call: its
  identity is verified and its tenant resolved again.
- **Stops at the first failure.** The failed line is the last one written, and its exit code,
  by the table below, is the batch's. Exit 0 means every line succeeded.
- **No idempotency key.** One key would claim every line, so `--idempotency-key` is refused. A
  retryable failure therefore exits 75 from a query and 69 from a command.
- **Signals.** A signal stops the batch: the line in flight gets `--grace` as a single call
  does, and no line starts after it. When lines remained, the batch exits 130 or 143.
- **The batch as a whole.** A refusal before any line runs (a conflicting option, an unreadable
  file, a runtime that does not start) is one error object without `line`.

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
retryable kinds are `concurrency`, `throttled`, `infrastructure` and `unavailable`.

## Error codes the CLI adds

| Code | Kind | Exit | Raised when |
|---|---|---|---|
| `cli_unknown_operation` | `validation` | 2 | the operation is unknown, or not exposed |
| `cli_input_not_accepted` | `validation` | 2 | `--input`, typed flags or `--batch` given to an operation that takes none |
| `cli_option_conflict` | `validation` | 2 | typed flags given with `--input`; `--batch` given with `--input`, typed flags, `--idempotency-key` or `--pretty` |
| `cli_flags_unsupported` | `validation` | 2 | typed flags given to an operation whose input takes none; the detail says why |
| `cli_malformed_flags` | `validation` | 2 | the typed flags do not parse: an unknown flag, a missing value or one spelled like a flag, a value to a switch, a field given twice, or an argument that is not a flag |
| `cli_input_unreadable` | `validation` | 2 | the `@PATH` file of `--input` or `--batch` cannot be read |
| `cli_malformed_input` | `validation` | 2 | the input, or a batch line, is not JSON, or not an object |
| `cli_inline_secret` | `validation` | 2 | the input declares a secret and was given inline or as typed flags |
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
