"""Cross-backend parity for the extended aggregate functions.

``$count_distinct`` / ``$stddev_pop`` / ``$stddev_samp`` / ``$var_pop`` / ``$var_samp`` are
exact on every backend. ``$percentile`` is exact on Postgres and the mock but *approximate*
on Mongo (its native estimator) — so percentile parity is checked only where exact
(``exclude_approx``), matching the documented contract.
"""

from __future__ import annotations

import math
from typing import Any

from tests.support.aggregate_having import AggCreate, AggDoc, AggRead, SEED

__all__ = [
    "AggCreate",
    "AggDoc",
    "AggRead",
    "SEED",
    "FUNCTION_CASES",
    "assert_aggregate_function_parity",
]

# region: east cnt=3 amounts[5,10,20] · west cnt=2 [50,100] · north cnt=2 [3,7] · south cnt=1 [40]
FUNCTION_CASES: tuple[dict[str, Any], ...] = (
    {
        "aggregates": {
            "$groups": {"region": "region"},
            "$computed": {
                "cnt": {"$count": None},
                "distinct_tier": {"$count_distinct": "tier"},
                "sp": {"$stddev_pop": "amount"},
                "ss": {"$stddev_samp": "amount"},
                "vp": {"$var_pop": "amount"},
                "vs": {"$var_samp": "amount"},
            },
        },
        "group_keys": ["region"],
        "metrics": ["cnt", "distinct_tier", "sp", "ss", "vp", "vs"],
        "approx_metrics": (),
    },
    {
        "aggregates": {
            "$groups": {"region": "region"},
            "$computed": {
                "p50": {"$percentile": {"field": "amount", "p": 0.5}},
                "p90": {"$percentile": {"field": "amount", "p": 0.9}},
            },
        },
        "group_keys": ["region"],
        "metrics": ["p50", "p90"],
        "approx_metrics": ("p50", "p90"),
    },
)


def _by_group(hits: list[Any], group_keys: list[str]) -> dict[tuple[Any, ...], Any]:
    return {tuple(row[k] for k in group_keys): row for row in hits}


def _assert_metric(real: Any, expected: Any, *, where: str) -> None:
    if expected is None or real is None:
        assert real == expected, f"{where}: {real!r} != {expected!r}"
        return

    # Every metric here is numeric; compare with tolerance so exact counts pass and
    # float-valued stats (mock float/Fraction vs PG Decimal vs Mongo double) agree.
    assert math.isclose(float(real), float(expected), rel_tol=1e-9, abs_tol=1e-9), (
        f"{where}: {real!r} not close to {expected!r}"
    )


async def assert_aggregate_function_parity(
    real_query: Any,
    oracle: Any,
    *,
    exclude_approx: bool,
) -> None:
    """Assert *real_query* reproduces the mock *oracle* for every function case.

    Both must already be seeded with :data:`SEED`. When *exclude_approx* is set (Mongo),
    approximate metrics are checked for group coverage only, not value equality.
    """

    for case in FUNCTION_CASES:
        real = await real_query.aggregate_page(
            aggregates=case["aggregates"], pagination={"limit": 100}
        )
        expected = await oracle.aggregate_page(
            aggregates=case["aggregates"], pagination={"limit": 100}
        )

        real_rows = _by_group(real.hits, case["group_keys"])
        exp_rows = _by_group(expected.hits, case["group_keys"])

        assert set(real_rows) == set(exp_rows), (
            f"group mismatch: {sorted(real_rows)} != {sorted(exp_rows)}"
        )

        metrics = [
            m
            for m in case["metrics"]
            if not (exclude_approx and m in case["approx_metrics"])
        ]

        for group, exp_row in exp_rows.items():
            for metric in metrics:
                _assert_metric(
                    real_rows[group][metric],
                    exp_row[metric],
                    where=f"{group}.{metric}",
                )
