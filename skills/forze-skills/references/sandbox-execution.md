# Running code out of process

Running a program — your own, or one generated at runtime — under governance: a declared threat tier, ceilings the backend enforces, a hard kill, and files that cross by storage key rather than by host path. The isolation tier is a wiring choice, and the boot refuses a combination that would run unreviewed code in something that cannot hold it.

## The threat declaration is the spec

```python
from forze.application.contracts.sandbox import ProgramPayload, SandboxRequest, SandboxSpec

recipes = SandboxSpec(name="recipes", provenance="untrusted")

# In a handler: the port is command-plane, so a QUERY operation cannot acquire one.
result = await ctx.sandbox.run(recipes).run(
    SandboxRequest(
        program=ProgramPayload(interpreter=("python",), source=generated_source),
        input_files={"data.csv": staged_key},
        output_globs=("report.json",),
    )
)
# A non-zero exit is a result, not an exception: `result.outcome`, `result.exit_code`,
# `result.output_files` (storage keys), `result.stdout.text` (bounded, secrets masked).
```

`provenance` has no default. `"trusted"` is a program you ship or a human reviewed; `"untrusted"` is generated or user-supplied, and it fails the boot on any adapter below the container tier.

## Trusted programs: the subprocess tier

```python
from datetime import timedelta

from forze.application.integrations.sandbox import (
    SubprocessSandboxConfig,
    SubprocessSandboxDepsModule,
)

jobs = SubprocessSandboxDepsModule(
    routes={
        "reports": SubprocessSandboxConfig(
            provenance="trusted",
            wall_clock_ceiling=timedelta(minutes=5),
            max_output_bytes=256 * 1024,
            storage=blobs_spec,
            memory_ceiling=512 * 1024 * 1024,
            cpu_ceiling=timedelta(seconds=30),
            # A bare child inherits this host's connectivity and nothing here changes that,
            # so every route wired to this adapter acknowledges it.
            acknowledge_network_egress=True,
        )
    }
)
```

A bare child shares the host's filesystem, network and `/proc`. The ceilings bound accidents, not adversaries; the tier is for your own binaries, hard-killed and cleaned up.

## Untrusted code: the container tier

```python
from datetime import timedelta

from forze_sandbox.container import ContainerSandboxConfig, ContainerSandboxDepsModule

sandboxes = ContainerSandboxDepsModule(
    routes={
        "recipes": ContainerSandboxConfig(
            provenance="untrusted",
            image="my-registry/recipe-runner:2026.09",  # already on the daemon; nothing pulls
            wall_clock_ceiling=timedelta(minutes=2),
            max_output_bytes=256 * 1024,
            storage=blobs_spec,
            memory_ceiling=512 * 1024 * 1024,
            cpu_ceiling=timedelta(seconds=30),
        )
    }
)
```

The container runs with no network, no capabilities, a pid ceiling and a non-root user; the workspace crosses as tar in both directions, and only declared `output_globs` leave it. A memory over-run comes back as `killed_oom` and a CPU over-run as `killed_resource`, because the daemon watches the ceilings from outside the child — the subprocess tier can impose them and cannot name them.

Set `network="egress"` only with `acknowledge_network_egress=True`: with staged inputs in the workspace, network access is an exfiltration path for them.

## Streaming, and stopping

```python
from contextlib import aclosing

async with aclosing(port.run_stream(request)) as events:
    async for event in events:
        if event.kind == "result":
            outcome = event.result
```

Exactly one `result` event, and it is last. Abandoning the generator ends the run — use `aclosing` so that happens where you chose rather than whenever the generator is collected.

## Choosing between this and `run_cpu`

`run_cpu` keeps the loop responsive for your own trusted compute; cancellation there is cooperative and a crash takes the worker. Reach for the sandbox plane when you need a hard kill, crash isolation, or you do not fully trust what runs.

## Reference

- [Sandbox execution](https://morzecrew.github.io/forze/latest/in-depth/data-events/sandbox/)
