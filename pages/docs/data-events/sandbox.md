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
  spawn. Argv is world-readable on the host; `ps` is not a privilege.
- **The child's environment is what you named.** Plus the route's declared
  passthrough list, which defaults to `("PATH",)`. Inheriting the worker's
  environment would hand every credential in it to the code you are distrusting.

## What ships today

| Tier | Adapter | Isolation | Runs |
| --- | --- | --- | --- |
| Base | `SubprocessSandbox` | `none` | trusted code only — the gate refuses it anything else |
| Container | recorded | `container` | untrusted code, against a consumer's own container infra |
| Remote | recorded | `container` / `vm` | managed sandboxing services |

The base adapter shares this host's kernel, filesystem, network and user with
the child. It declares exactly that, which is why wiring it also costs an
explicit `acknowledge_network_egress=True`: it cannot close the network, so it
does not claim to.

```python
from forze.application.integrations.sandbox import (
    SubprocessSandboxConfig, SubprocessSandboxDepsModule,
)

SubprocessSandboxDepsModule(          # registers SandboxDepKey ("sandbox_run") per route
    routes={
        "recipes": SubprocessSandboxConfig(
            provenance="trusted",
            wall_clock_ceiling=timedelta(seconds=60),   # required; no unbounded default
            max_output_bytes=1_000_000,                 # required; a chatty child is bounded
            acknowledge_network_egress=True,
            storage=RECIPE_FILES,
        ),
    },
)
```

Its hard kill is as complete as its tier: it kills the process it started, and a
child that forked its own children can orphan them. Only the container and vm
tiers guarantee that killing the sandbox reaps everything it spawned.

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
