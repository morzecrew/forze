---
title: Operations CLI
icon: lucide/terminal
summary: Run your operations from a terminal, a cron job or a Kubernetes Job, governed as over HTTP
---

Admin work tends to happen in one-off scripts: requeue the failed runs, fix a tenant, backfill
a column. Each script builds a runtime and calls a handler, or goes straight to the database,
outside authz, tenancy, read-only enforcement, idempotency and audit. `forze ops` is the front
door those scripts lack. It lists, describes and calls the operations your registry already
holds. Every call validates its input, verifies who is calling, resolves the tenant and runs
through the same pipeline an HTTP or MCP caller goes through.

## Declare what the CLI exposes

An `OpsApp` names the registry, the runtime a call runs on, and who calls run as. Exposure
follows MCP: queries only by default, commands with `include_writes=True`, and an optional
allowlist that is checked like a grant. Sensitive read models are refused.

```python
--8<-- "recipes/ops_cli/app.py:declaration"

--8<-- "recipes/ops_cli/app.py:ops"
```

The declaration is checked when it is built. An unknown allowlisted key, a command without
`include_writes`, or a sensitive operation fails while your module imports. `forze_cli.ops`
imports neither Typer nor anything else from the `cli` extra, so the web process can import the
same module.

!!! warning "Unguarded operations become reachable"
    An operation that relies on its route for protection has none here. The tenancy admin
    kit, for one, ships unguarded by design. When `include_writes` is on, keep an allowlist.

A sensitive operation fails the declaration even when you didn't name it, because the default
takes every query. The storage kit's `presign_download` is one, since its result is a bearer
credential. An `OpsApp` over a registry that holds the storage kit needs an `operations=`
allowlist that leaves it out.

## Build a one-shot runtime

`runtime` is a factory. `list` and `describe` never call it, so they need no credentials and
open no pool. `call` builds the runtime once, enters its scope, runs the operation and shuts
the runtime down.

```python
--8<-- "recipes/ops_cli/app.py:runtime"
```

The runtime must be `DeploymentProfile.SERVERLESS`, or `call` exits 78. That profile refuses
lifecycle steps declared `requires_long_running` and drops the drain window to zero, which is
what a process that exits after one call needs. The check trusts the declaration, though. A
poller whose author never declared it still starts, so build this runtime from only the deps
and steps the exposed operations use.

## Choose who a call runs as

The declaration must name an identity. There is no default, and no option picks a principal at
run time.

| Identity | Binds | Use it for |
|---|---|---|
| `ApiKeyIdentity(authn_spec, env="FORZE_API_KEY", tenancy_route=None)` | the principal a verified key authenticates, and its tenant | per-operator or per-job keys: real attribution, revocable |
| `AnonymousIdentity()` | nothing | operations with no authn requirement |
| `StaticIdentity(authn=…, tenant=…, acknowledge_unverified_identity=True)` | a fixed identity nothing verifies | single-tenant tools, break-glass inside the cluster |

**API key.** The key is read from the environment variable `env`, or from
`--api-key-file PATH`. It is never read from argv, which `ps` and shell history show. It is
verified through the same authn port HTTP and MCP use, so expiry, revocation, eligibility and
delegation keys behave the same on every surface. The tenant resolves from the principal on
`tenancy_route`, which defaults to the authn spec's name. `--tenant UUID` *requests* a tenant;
the resolver validates it against the principal's membership.

**Static identity.** It costs three things, so it is refused unless you acknowledge it:

- there is no expiry, revocation or eligibility check;
- every operator shares one audit actor;
- every caller shares one idempotency namespace. Two jobs passing the same
  `--idempotency-key` share one claim, so prefix keys with the job's name.

`--tenant` is refused with an anonymous or static identity, because there is nothing to
validate it against.

## Call an operation

```console
$ export FORZE_OPS_APP=myapp.ops:ops
$ forze ops list
$ forze ops describe notes.create
$ forze ops call notes.create --input '{"title": "hi"}'
{"id":"…","title":"hi",…}
```

Run `forze --install-completion` once, and your shell completes operation names after
`describe` and `call`. Each TAB imports your application module, so keep its import free of
side effects.

`--input` takes inline JSON, `@PATH` for a file, or `-` for stdin. Inline input is refused for
an operation whose input declares a `SecretStr` or `SecretBytes` field; pipe it instead. An
unknown input key is refused, not dropped, so a typo in a script fails loudly.

An operation whose input is flat scalars (strings, numbers, booleans, ids, dates, enums) also
takes typed flags instead of `--input`:

```console
$ forze ops call notes.create --title hi
```

Each field is a flag named after it, a boolean is `--name` or `--no-name`, and the values are
validated exactly as `--input` is. `forze ops describe` lists an operation's flags, or says why
it has none; the [reference](../reference/ops-cli.md#typed-flags) has the rules. Flags put every
value on argv, where `ps` and shell history show it: secret fields are refused, and any other
sensitive value belongs in `--input @PATH`.

`call` writes exactly one JSON document to stdout: the result, or an error object carrying the
invocation's `execution_id` and `correlation_id`. Everything else the application writes,
logs and prints included, goes to stderr. Branch on the exit code: 0 is success, 4 a rejected
input, 5 a denial, 75 a failure safe to retry as is. The
[reference](../reference/ops-cli.md#exit-codes) has the whole table.

For a command a retry may repeat, pass `--idempotency-key`. A repeat with the same key and
input returns the stored result; the same key with different input exits 4. An operation that
does not support a key refuses the option, so a script never believes a retry is safe when it
is not.

## Run a backfill

`--batch` calls an operation once per line of a JSONL file, or of stdin, inside one runtime,
so a backfill pays for startup once rather than per row:

```console
$ forze ops call notes.create --batch @notes.jsonl > results.jsonl
```

Every line is validated before anything runs, so a typo on line 900 does not leave 899 rows
applied. Stdout gets one JSON line per call, numbered by its input line, and the batch stops at
the first failure: that line is the last one written, and its exit code is the batch's. To
resume, fix that line and run the file from it on. `--idempotency-key` is refused with
`--batch`, because one key would claim every line. The
[reference](../reference/ops-cli.md#batches) has the output contract.

## Run it as a Job or from cron

Run the command where the application's infrastructure credentials already live, in the
cluster, not on a laptop. A Kubernetes Job can retry on exit 75 alone, the code that means
"repeating cannot apply anything twice", and fail at once on anything else:

```yaml
apiVersion: batch/v1
kind: Job
spec:
  backoffLimit: 3           # retries of exit 75 count against this
  podFailurePolicy:
    rules:
      - action: FailJob     # any other failure is final
        onExitCodes: { operator: NotIn, values: [75] }
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: reconcile
          image: myapp:latest
          command: ["forze", "ops", "call", "billing.reconcile", "--input", "@/config/reconcile.json"]
          env:
            - { name: FORZE_OPS_APP, value: myapp.ops:ops }
            - name: FORZE_API_KEY
              valueFrom: { secretKeyRef: { name: reconcile-job-key, key: api-key } }
```

A retryable failure of a command called *without* a key exits 69 instead: a connection lost
during `COMMIT` may have landed the write. Keep 69 out of automatic retries.

## Interrupts

The first SIGINT or SIGTERM does not cancel the call. It waits up to `--grace` seconds (10 by
default) for the call to finish, because cancelling a commit leaves its outcome unknown. A call
that finishes in time exits with its own code. A second signal, or the grace running out,
cancels it:

- before any commit started, the error object's kind is `interrupted`, and the exit code is
  130 or 143;
- at or after a commit point, it is `commit_ambiguous`, exit 1. Reconcile before re-running.

Shutdown runs either way. Set the grace below your scheduler's own: Kubernetes'
`terminationGracePeriodSeconds` defaults to 30.

## Ship it inside your own CLI

`build_ops_cli(ops)` returns the same command group with your declaration bound, so
`myapp ops call …` needs no `--app`:

```python
import typer

from forze_cli.ops import build_ops_cli

from myapp.ops import ops

cli = typer.Typer(pretty_exceptions_show_locals=False)
cli.add_typer(build_ops_cli(ops))
```

!!! danger "Never print locals"
    Set `pretty_exceptions_show_locals=False` on your root `typer.Typer`. Typer reads it from
    the app you invoke, never from a mounted group, and a crash that prints locals prints the
    credentials an operation's frames hold.

The output contract, every option, and the error codes the CLI adds are in the
[Operations CLI reference](../reference/ops-cli.md).
