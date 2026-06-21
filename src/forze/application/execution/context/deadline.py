"""Task-scoped invocation deadline.

The deadline primitive now lives in :mod:`forze.base.primitives.deadline` (the
lowest layer) so base-level seams — the process-wide resilience executor, port
wrappers, the CPU offload primitive — can read it without importing upward. This
module re-exports it unchanged for the application layer; the ContextVar is a
single shared instance.
"""

from forze.base.primitives.deadline import (
    bind_deadline,
    current_deadline,
    remaining_time,
    reset_deadline,
    set_deadline,
)

# ----------------------- #

__all__ = [
    "bind_deadline",
    "current_deadline",
    "remaining_time",
    "reset_deadline",
    "set_deadline",
]
