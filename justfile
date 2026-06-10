set quiet
set shell := ["bash", "-cu"]

# ----------------------- #
# Paths / constants

_uv_sync := "uv sync --all-groups --all-extras > /dev/null 2>&1"

# ....................... #

_pwd := justfile_directory()
_cwd := join(_pwd, "pages")

_d2_dir := join(_cwd, "diagrams")
_d2_light_build_dir := join(_cwd, "docs", "_diagrams", "light")
_d2_dark_build_dir := join(_cwd, "docs", "_diagrams", "dark")
_d2_light_flags := "--center --scale 1"
_d2_dark_flags := "--theme 200 --center --scale 1"

# ----------------------- #
# Default command

[no-exit-message]
_default:
    echo "Available commands:"
    echo
    just --color=always --list | sed '1d'

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

# Run fast tests
test *args='':
    {{ _uv_sync }}

    uv run pytest -m "not perf" {{ args }}

# Run performance benchmarks (-m perf; Docker only where a perf conftest starts containers)
perf *args='tests/perf':
    {{ _uv_sync }}

    uv run pytest \
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

    just _uv_cmd "Linting" {{ strict }} ruff check "src"
    just _uv_cmd "Types" {{ strict }} mypy "src"
    just _uv_cmd "Imports" {{ strict }} lint-imports
    just _uv_cmd "Dead code" {{ strict }} vulture
    just _uv_cmd "Dependencies" {{ strict }} deptry .
    just _uv_cmd "Security" {{ strict }} bandit -c pyproject.toml -r "src"
    just _uv_cmd "Frozen bypass" {{ strict }} pre-commit run no-frozen-setattr-bypass --all-files
    just _uv_cmd "Secrets" {{ strict }} pre-commit run gitleaks --all-files


# ----------------------- #
# Docs

# Serve the documentation with live reload
[working-directory("pages")]
serve-docs:
    uv run zensical serve

# Build the documentation site (diagrams + zensical) into pages/site
[working-directory("pages")]
build-docs: build-diagrams
    uv run zensical build

# Build D2 diagrams
build-diagrams:
    mkdir -p {{ _d2_light_build_dir }}
    mkdir -p {{ _d2_dark_build_dir }}

    for f in {{ _d2_dir }}/*.d2; do \
        d2 "$f" "{{ _d2_light_build_dir }}/$(basename "${f%.d2}.svg")" {{ _d2_light_flags }}; \
        d2 "$f" "{{ _d2_dark_build_dir }}/$(basename "${f%.d2}.svg")" {{ _d2_dark_flags }}; \
    done

# ----------------------- #
# Utils

_worktree_dir := join(_pwd, "..", "worktrees")

# Create a worktree for a branch
[arg("new", long, value="true", help="Create a worktree for a new branch")]
worktree branch new="false":
    mkdir -p {{ _worktree_dir }}

    if {{ new }}; then \
        git worktree add {{ _worktree_dir }}/forze-{{ branch }} -b {{ branch }} main;
    else \
        git worktree add {{ _worktree_dir }}/forze-{{ branch }} {{ branch }};
    fi
