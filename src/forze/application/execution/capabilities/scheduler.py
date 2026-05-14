"""Topological sort for capability ``requires`` / ``provides`` within a bucket."""

from collections import defaultdict

from forze.base.errors import CoreError

from ..plan.spec import MiddlewareSpec

# ----------------------- #


def schedule_capability_specs(
    specs: tuple[MiddlewareSpec, ...],
    *,
    bucket: str,
) -> tuple[MiddlewareSpec, ...]:
    """Return ``specs`` reordered for capability constraints (per-bucket graph)."""

    if not specs:
        return specs

    n = len(specs)
    any_cap = any(s.requires or s.provides for s in specs)

    if not any_cap:
        return specs

    key_providers: dict[str, int] = {}

    for i, s in enumerate(specs):
        for k in s.provides:
            if k in key_providers:
                raise CoreError(
                    f"Capability {k!r} is provided by more than one step in bucket "
                    f"{bucket!r} (indices {key_providers[k]} and {i})"
                )

            key_providers[k] = i

    adj: defaultdict[int, set[int]] = defaultdict(set)
    indeg = [0] * n

    for j, sj in enumerate(specs):
        for k in sj.requires:
            if k not in key_providers:
                raise CoreError(
                    f"Bucket {bucket!r}: capability {k!r} is required by a step "
                    f"but no step in this bucket provides it"
                )

            i = key_providers[k]

            if i == j:
                raise CoreError(
                    f"Bucket {bucket!r}: step at index {j} both requires and provides {k!r}"
                )

            if j not in adj[i]:
                adj[i].add(j)
                indeg[j] += 1

    order: list[int] = []
    ready = [i for i in range(n) if indeg[i] == 0]
    ready.sort(key=lambda idx: (-specs[idx].priority, idx))

    while ready:
        u = ready.pop(0)
        order.append(u)

        for v in sorted(adj[u], key=lambda idx: (-specs[idx].priority, idx)):
            indeg[v] -= 1

            if indeg[v] == 0:
                ready.append(v)
                ready.sort(key=lambda idx: (-specs[idx].priority, idx))

    if len(order) != n:
        raise CoreError(
            f"Capability dependency graph in bucket {bucket!r} contains a cycle"
        )

    return tuple(specs[i] for i in order)
