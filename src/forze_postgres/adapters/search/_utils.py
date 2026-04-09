from typing import Any

from forze.application.contracts.search import SearchOptions, SearchSpec

# ----------------------- #


def calculate_effective_field_weights(
    spec: SearchSpec[Any],
    options: SearchOptions | None = None,
) -> dict[str, float]:
    options = options or {}
    provided_weights = options.get("weights", {})
    fields_to_search = list(options.get("fields", []))

    # First priority - provided weights
    if provided_weights:
        weights = {f: provided_weights.get(f, 0.0) for f in spec.fields}

    # Second priority - fields to search
    elif fields_to_search:
        weights = {f: 1.0 if f in fields_to_search else 0.0 for f in spec.fields}

    # First fallback - default weights
    elif spec.default_weights:
        weights = dict(spec.default_weights)

    # Last fallback - all fields with weight 1.0
    else:
        weights = {f: 1.0 for f in spec.fields}

    return weights
