---
title: Sandbox
icon: lucide/box
summary: Run code out-of-process under a fail-closed isolation gate, a hard kill you can rely on, and a seam a simulation can cut
---

Some products have to run a program they did not write when they were built: a
recipe an operator saved, a script an agent generated, a validation step that
must not be able to take the worker down with it. That work belongs outside the
process — and the moment it leaves, four things need answers. What contains it.
What happens when it will not stop. What it may read, and what may leave with
it. And what a test is supposed to do about any of it.

The **sandbox** plane answers those four, and is honest about the one it cannot:
a Python library cannot provide isolation. Real confinement is a container
runtime, a gVisor-class supervisor, a microVM or a remote execution service —
none of which ship inside `forze`.

## What the plane actually buys you

**A gate that fails closed.** A route declares whose code it runs. An adapter
declares what it confines. Untrusted code on an adapter that confines nothing
fails the boot, not the request:

```
Sandbox route 'recipes' runs untrusted code on backend 'subprocess', whose
isolation is 'none'. Untrusted provenance requires 'container' or stronger …
```

That refusal is the plane's single most valuable line. Every application that
runs generated code eventually runs it in something that is not a sandbox — the
bare adapter wired during a rushed local setup, a mock left in the wrong
profile. Declaring the threat once turns that into a failed boot.

**A red button that is real.** A deadline kills the child: `SIGTERM`, a grace
period, then `SIGKILL`, with the workspace cleaned on every exit path including
the killed one. This is the thing [`run_cpu`](../recipes/offload-cpu-work.md)
structurally cannot do — an uncheckpointed thread can only be abandoned.

**A seam a simulation can cut.** Out-of-process work is real wall-clock work off
the loop, which the deterministic simulator refuses outright. Under simulation
the port is answered by a function you register, so `killed_oom`,
`killed_timeout` and `spawn_failed` arrive on the call you choose instead of on
a bad day in production.

## The shape in code

```python
from forze.application.contracts.sandbox import SandboxSpec, SandboxRequest, ProgramPayload

RECIPES = SandboxSpec(name="recipes", provenance="trusted")

result = await ctx.sandbox.run(RECIPES).run(
    SandboxRequest(
        program=ProgramPayload(interpreter=("python3",), source=recipe_source),
        input_files={"data.csv": dataset_key},     # staged from storage by key
        output_globs=("out/*.parquet",),           # only these leave
        timeout=timedelta(seconds=30),
    )
)

if result.outcome == "exited" and result.exit_code == 0:
    publish(result.output_files["out/summary.parquet"])
```

`provenance` has no default: there is no honest one. Guessing `trusted` runs
generated code in a bare child; guessing `untrusted` fails the boot of every app
that only runs its own binaries.

**A failed run is a result, not an exception.** A non-zero exit, an OOM kill, a
deadline — each comes back as a `SandboxResult` with an `outcome`, because for a
generated program a non-zero exit is frequently the answer rather than an error.
Only the framework's own failures raise: a workspace it could not create, an
input it could not stage, an output it could not store.

## Four rules the plane does not bend

- **Programs, never callables.** `command` is argv; there is no shell string and
  no pickled callable. Shipping a Python function across the boundary means
  unpickling it on the far side, which is the trust boundary this plane exists
  to draw. `ProgramPayload` writes your source into the workspace and runs the
  interpreter on it — explicit, inspectable, loggable.
- **Files cross by key, never by path.** Inputs are staged from the storage
  plane into the workspace; declared outputs are collected back to it. Whatever
  the child wrote and nobody declared dies with the workspace, so "collect my
  results" is not an exfiltration channel.
- **Secrets ride the environment.** `env` accepts a `SecretRef`, resolved at
  spawn. Argv is world-readable on the host; `ps` is not a privilege. A resolved
  value is masked out of the captured output before the result is returned, so a
  child that echoes one — or dumps `os.environ` in a traceback — does not put it
  in a journal.
- **Only files inside the workspace leave.** Collection skips a symlink whatever
  it points at, and every resolved path must still land inside the workspace, so
  neither a link nor a linked directory sends a host file out under a
  workspace-relative name.
- **The child's environment is what you named.** Plus the route's declared
  passthrough list, which defaults to `("PATH",)`. Inheriting the worker's
  environment would hand every credential in it to the code you are distrusting.

## What ships today

| Tier | Adapter | Isolation | Runs |
| --- | --- | --- | --- |
| Base | `SubprocessSandbox`, no ceilings | `none` | trusted code only — the gate refuses it anything else |
| Process | `SubprocessSandbox` with ceilings or a dropped user | `process` | trusted code, bounded against accidents |
| Container | recorded | `container` | untrusted code, against a consumer's own container infra |
| Remote | recorded | `container` / `vm` | managed sandboxing services |

The first two rows are one adapter. Which tier a route gets is decided entirely
by its wiring: set a ceiling or a user and it is `process`, set neither and it is
`none`. **Neither is a security boundary** — the child still shares this host's
kernel, filesystem, network and `/proc`, so the provenance gate refuses both of
them untrusted code. Ceilings bound accidents, not adversaries.

Wiring either also costs an explicit `acknowledge_network_egress=True`: the
adapter cannot close the network, so it does not claim to.

### Ceilings, and what they can tell you afterwards

```python
SubprocessSandboxConfig(
    ...,
    memory_ceiling=512 * 1024 * 1024,      # RLIMIT_AS
    cpu_ceiling=timedelta(seconds=30),     # RLIMIT_CPU
    open_files_ceiling=256,                # RLIMIT_NOFILE
    run_as_user="sandbox",                 # needs a root worker; refused at freeze otherwise
)
```

They are applied by re-execing through a small shim, so nothing runs in the
worker between the fork and the exec. `run_as_user` needs a root worker unless
it names the identity the worker already has, and the workspace is handed to
that user before the child starts — `mkdtemp` would otherwise leave a directory
the child cannot enter.

A memory ceiling bounds the program's whole address space, interpreter and
shared libraries included, so one under about 64 MiB stops a Python child before
its first line. That comes back as a failed start rather than as anything about
memory, which is why the limits in force are named in `SandboxResult.detail`.

**A ceiling that bites is not a ceiling that reports.** An `RLIMIT_AS` breach is
the child's own `MemoryError` and an `EMFILE` is its own `OSError` — identical to
the same program failing with no limit at all. So the capability model splits the
two claims: `enforces_memory` means the ceiling is *imposed*, and
`reports_resource_kill` means an over-run is *identifiable*. This tier declares
the first and not the second, and names the limits that were in force in
`SandboxResult.detail` so a reader of an exit 1 can see what was bounding it.

The exception is CPU: `SIGXCPU` comes from nowhere else, so a run over that
ceiling comes back as `killed_resource`. If you need that for memory, you need a
tier that watches the ceiling from outside the child.

```python
from forze.application.integrations.sandbox import (
    SubprocessSandboxConfig, SubprocessSandboxDepsModule,
)

SubprocessSandboxDepsModule(          # registers SandboxDepKey ("sandbox_run") per route
    routes={
        "recipes": SubprocessSandboxConfig(
            provenance="trusted",
            wall_clock_ceiling=timedelta(seconds=60),   # required; no unbounded default
            max_output_bytes=1_000_000,                 # required; per stream, so the run's total is twice it
            acknowledge_network_egress=True,
            storage=RECIPE_FILES,
            max_artifact_bytes=64 * 1024 * 1024,        # defaulted; declared outputs read into the worker
            max_artifact_count=1024,                    # defaulted; a glob over it collects nothing
        ),
    },
)
```

### The kill takes the group

Every child is spawned into a session of its own, so the kill goes to its process
group: a child that forked its own children takes them with it. Where the
platform has no process groups the signal reaches the child alone, and
`reaps_descendants` says which you have rather than assuming.

That is still not the container tier's guarantee. A descendant that escapes the
group — one that called `setsid` itself — outlives the run, and only namespaced
pids close that off for good.

### Streaming

`run_stream` yields the child's output as it arrives, then one final `result`
event carrying everything `run` would have returned. `run` is the same code with
nobody watching the chunks, so the two cannot answer differently about one run.

**Closing the generator kills the child's process group**, drains its pipes and
removes its workspace. A bare `break` is not a close — Python finalizes an
abandoned generator when the last reference goes, which under asyncio lands a
turn or so later, and a caller holding the generator in a variable keeps the
child alive until it lets go. Use `contextlib.aclosing` and the moment is yours:

```python
async with aclosing(ctx.sandbox.run(RECIPES).run_stream(request)) as events:
    async for event in events:
        if event.kind == "result":
            outcome = event.result
```

The streamed chunks carry everything the child wrote; the result's captured
streams are capped as they are for `run`. The cap exists because the result is
held in memory and journaled, and a chunk handed straight to a caller is neither.

## Testing against it

```python
from forze_mock import MockSandboxRegistry

registry = MockSandboxRegistry().on(
    "recipes",
    lambda request: SandboxResult(outcome="killed_oom"),
)
```

The mock executes nothing — that is the point of the seam — so every route needs
a registered answer, and an unprogrammed one refuses rather than inventing a
result. Register the real backend's capabilities alongside it
(`registry.on(..., capabilities=...)`) when a route stands in for a specific
adapter: the gates then refuse under test exactly where production would.

## Limits

- **No isolation ships in the box.** The base adapter is process management with
  governance around it. If you need containment, the container tier is where it
  lives, and it lands with the infrastructure it wraps.
- **`process`-tier enforcement is not here yet.** Memory and CPU ceilings, uid
  drop and process-group kill arrive with the tier that declares them — no
  adapter claims `enforces_memory` without a test that drives a child into it.
- **Streaming is refused, not buffered.** `run_stream` is served by adapters
  that declare it; the base adapter says no rather than pretending.
- **A killed run still hands back what it wrote.** Declared outputs are collected
  after a kill as well as after a clean exit — a half-written artifact is usually
  the most useful thing about a run that did not finish, and the `outcome` beside
  it says plainly that it did not.
