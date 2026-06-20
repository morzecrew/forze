---
title: Crashes & partitions
icon: lucide/power-off
summary: The harder failure modes — kill the process mid-flush and restart over the persisted store, or split N real runtimes apart with a network partition, all from one seed
---

Faults and latency perturb a process that keeps running. The failures that corrupt state are the ones that stop it: a process that *dies* mid-write, or a network that *splits* a cluster in two. DST drives both — a single-process crash-and-restart, and a multi-runtime cluster under network partitions — from the same master seed, against the same invariants.

## Crash and restart

Set a `CrashPolicy` and a run becomes a crash → restart → recovery scenario. The process *dies* at a matched port boundary — the in-flight transaction rolls back, committed state persists — then a fresh runtime restarts over the **same persisted store**, an optional `recover` pass redrives interrupted work, and the invariants check the post-recovery world:

```python
from forze_dst import SimulationConfig
from forze_dst.faults import CrashPolicy

report = simulation.run(SimulationConfig(
    seeds=range(64),
    crash=CrashPolicy(surface="document_command", route="orders", op="update"),
))
```

This is how you catch recovery bugs. A charge committed *before* a crashed follow-up write leaves an orphan that survives the restart when the operation is non-transactional — while the same operation routed through a transaction rolls the partial write back atomically. The crash point is seeded, so the bug reproduces at the exact boundary that produced it.

## Across N nodes

A `Cluster` runs *N* real runtimes over one shared store, from one master seed, under **network partitions** — the distributed capstone. A partition cuts a node-group off from the gated port surfaces for a window of virtual time (modelled as *unreachable* — a retryable error), so a correct retry/outbox flow heals while a fire-and-forget one loses work. The distributed invariants are the same ones: `mutual_exclusion` for no split-brain, `no_duplicate_effect` for exactly-once.

```python
from forze_dst import Cluster, SimulationConfig
from forze_dst.cluster import ClusterConfig, Partition, PartitionSchedule
from forze_mock import MockDepsModule
from forze_mock.state import MockState

async def node(node_id: int, ctx) -> None:
    ...  # this node's work, over ctx ports on the shared store

report = Cluster(
    deps=lambda state: MockDepsModule(state=state),
    node=node,
    state_factory=MockState,
    invariants=[...],
).run(SimulationConfig(
    seeds=range(32),
    cluster=ClusterConfig(
        nodes=3,
        partitions=PartitionSchedule(
            windows=(Partition(start=0.5, end=1.5, isolated=frozenset({1})),),
            surfaces=frozenset({"queue_command"}),
        ),
    ),
))
```

On a violation the cluster minimises by **dropping nodes** — the smallest cluster that still breaks, usually two.

A window's `loss` turns a clean cut into a **flaky link**: `Partition(start=0.5, end=1.5, isolated=frozenset({1}), loss=0.3)` drops 30 % of node 1's gated calls (seeded) instead of all of them, and overlapping windows on different node-groups give an **asymmetric** split. `loss=1.0` (the default) is the hard partition above.

These scenarios generate large interleaving spaces — a crash can land at any matched boundary, a partition can strike at any point in its window. [Exploration strategies](dst-exploration.md) is how DST searches that space efficiently instead of by brute luck.
