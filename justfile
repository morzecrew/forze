set quiet
set shell := ["bash", "-cu"]

# ----------------------- #
# Paths / constants

_uv_sync := "uv sync --all-groups > /dev/null 2>&1"

_publish_url := "https://pyoci.com/ghcr.io/morzecrew/"

# ----------------------- #
# Default command

_default:
    just --list

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

# Run tests
test *args='':
    {{ _uv_sync }}

    uv run pytest -- {{ args }}


# Run quality checks
[arg("strict", long, short="s", value="true", help="Enable strict mode (fail on error in any check)")]
quality strict="false":
    {{ _uv_sync }}

    just _uv_cmd "Types" {{ strict }} ruff check "src"
    just _uv_cmd "Imports" {{ strict }} lint-imports
    just _uv_cmd "Dead code" {{ strict }} vulture
    just _uv_cmd "Dependencies" {{ strict }} deptry .
    just _uv_cmd "Security" {{ strict }} bandit -c pyproject.toml -r "src"


# ----------------------- #

# Publish
publish username password:
    uv build
    uv publish \
        --publish-url {{ _publish_url }} \
        --username {{ username }} \
        --password {{ password }}