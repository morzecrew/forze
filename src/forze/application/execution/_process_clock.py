"""The process CPU clock, read raw for ``instrument_process``'s ``process.cpu.time``.

The determinism guard bans ``os.times`` everywhere else and allowlists only this module:
the value is metric output, never program logic — the reason ``perf_counter`` is allowed.
"""

import os

# ----------------------- #


def process_cpu_seconds() -> tuple[float, float]:
    """Return the CPU time this process has used so far.

    :returns: ``(user, system)`` seconds.
    """

    times = os.times()

    return times.user, times.system
