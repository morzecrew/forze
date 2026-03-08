set quiet
set shell := ["bash", "-cu"]

# ----------------------- #
# Modules

# Supported commands: serve, build, diagrams
mod pages "pages/justfile"

# ----------------------- #
# Paths / constants

_uv_sync := "uv sync --all-groups --all-extras > /dev/null 2>&1"

# ----------------------- #
# Default command

[no-exit-message]
_default:
    echo "Available commands:"
    echo
    just --color=always --list | sed '1d; /^\s*pages\b/d'
    echo
    echo "Pages module commands:"
    echo
    just --color=always --list pages | sed '1d'

[private]
help:
    just

# ----------------------- #
# Helpers

# Run a command and print the result based on the output
[no-cd]
_uv_cmd name strict *command:
    @printf "%-30s" "{{ name }}..."

    @out="/tmp/{{ name }}.$$$$" \
    trap 'rm -f "$$out"' EXIT; \
    if uv run {{ command }} >"$$out" 2>&1; then \
        echo "✅"; \
    else \
        echo "❌"; \
        echo ""; \
        cat "$$out"; \
        echo ""; \
        if {{ strict }}; then \
            exit 1; \
        fi; \
    fi

# ----------------------- #
# CI

# Run tests
test *args='':
    {{ _uv_sync }}

    uv run pytest {{ args }}

# Run fast tests
test-fast *args='':
    {{ _uv_sync }}

    uv run pytest -m "not perf" {{ args }}

# Run performance tests (requires Docker for testcontainers)
test-perf *args='':
    {{ _uv_sync }}

    uv run pytest tests/perf \
        --benchmark-only \
        --benchmark-columns=min,mean,max \
        --benchmark-time-unit=ms \
        --benchmark-max-time=30 \
        -m perf \
        {{ args }}
    

# Run all quality checks
[arg("strict", long, short="s", value="true", help="Enable strict mode (fail on error in any check)")]
quality strict="false":
    {{ _uv_sync }}

    just _uv_cmd "Types" {{ strict }} ruff check "src"
    just _uv_cmd "Imports" {{ strict }} lint-imports
    just _uv_cmd "Dead code" {{ strict }} vulture
    just _uv_cmd "Dependencies" {{ strict }} deptry .
    just _uv_cmd "Security" {{ strict }} bandit -c pyproject.toml -r "src"