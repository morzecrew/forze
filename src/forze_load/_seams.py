"""The two raw primitives the load kit needs, kept apart so the rest stays guarded.

The kit drives a live app and never runs under simulation, so it builds its own seeded RNGs
and reads the process's CPU time. Everything else in ``forze_load`` stays under the
determinism guard (``tests/unit/test_determinism_guard.py``), which allowlists this file
only.
"""

from __future__ import annotations

import random
import time

# ----------------------- #


def seeded_random(seed: int, stream: str) -> random.Random:
    """An RNG for one named stream of the run, so picks and arrivals do not share draws."""

    # A str seed goes through SHA-512 in `random.seed`, so it is stable across processes.
    return random.Random(f"{seed}/{stream}")  # nosec B311 - seeded load picks, not crypto


def process_seconds() -> float:
    """CPU seconds the whole process has used, every thread included."""

    return time.process_time()
