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

    uv run pytest -m "not perf and not fuzz" {{ args }}

# Verify the conformance manifest: every plane triaged, every declared leg collectable.
# Offline (imports + a pytest collection pass, no containers) — this is the line `just
# quality` runs, and the one that fails when a backend registers a manifested port
# without a leg.
conformance-check:
    {{ _uv_sync }}

    uv run python .github/scripts/conformance_manifest.py --collect

# Docs floors: every public contract symbol is mentioned by some page, the nav resolves
# both ways, and every relative link between docs lands. Offline; part of `just quality`.
docs-check:
    {{ _uv_sync }}

    uv run python .github/scripts/docs_floors.py

# Skills corpus integrity: every python example parses, every `forze*` symbol it imports
# still exists, structure and index parity hold, and no link escapes the published tree.
# The corpus ships into other people's repositories and is read by agents that write code
# from it, so a renamed export there is a broken import somewhere nobody here can see.
# Offline; part of `just quality`. Published-link liveness is `just skills-links`.
skills-check *args='':
    {{ _uv_sync }}

    uv run python -m tools.skills_check {{ args }}

# Sweep the corpus's published doc URLs for HTTP 200. Network-bound and rate-limited, so
# it runs on a schedule rather than per change — docs links break for reasons a given
# change does not control, which belongs on a queue and not in a merge gate.
#
# No `uv sync` and no venv, like the nightly verdict job: this mode reads Markdown and
# speaks HTTP through the standard library alone, so the sweep cannot fail for want of an
# extra — and the scheduled run executes the same line this recipe does.
skills-links *args='':
    python3 -m tools.skills_check --links {{ args }}

# Run the differential conformance legs themselves and gate on what actually ran (needs
# Docker for every real engine). CI does this across its per-integration shards instead of
# one job — starting every engine's containers at once peaks well past a 16 GB runner,
# which is the same reason the test matrix is sharded at all — and unions the per-shard
# files in the `conformance` job. This recipe is the single-machine equivalent.
conformance *args='':
    {{ _uv_sync }}

    rm -rf .conformance && mkdir -p .conformance

    uv run pytest -m "conformance and not perf and not fuzz" \
        --conformance-executed=.conformance/local.json {{ args }}
    uv run python .github/scripts/conformance_manifest.py --executed .conformance

# Save a local perf baseline for the gated (in-process) benchmark subset
perf-save:
    {{ _uv_sync }}

    uv run pytest \
        --benchmark-only \
        --benchmark-warmup=on \
        --benchmark-disable-gc \
        --benchmark-save=local \
        -m perf_gate \
        tests/perf

# Compare the gated benchmark subset against the saved local baseline (fail >10% on min)
perf-check:
    {{ _uv_sync }}

    uv run pytest \
        --benchmark-only \
        --benchmark-warmup=on \
        --benchmark-disable-gc \
        --benchmark-compare \
        --benchmark-compare-fail=min:10% \
        --benchmark-columns=min,mean,max \
        --benchmark-time-unit=ms \
        -m perf_gate \
        tests/perf

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


# Run the extended DST fuzz (64/128 seeds vs the merge guard's 8/12; excluded from `just test`)
fuzz *args='tests/unit/test_forze_dst':
    {{ _uv_sync }}

    uv run pytest -m fuzz {{ args }}


# Run one cell of the nightly DST matrix (see `just dst-nightly-cells` for the names)
dst-nightly cell seeds='65536':
    {{ _uv_sync }}

    uv run python .github/scripts/dst_nightly.py \
        --cell {{ cell }} \
        --seeds {{ seeds }} \
        --out nightly-{{ cell }}.json


# List the cells the nightly matrix runs — derived from the declared fault profiles
dst-nightly-cells:
    {{ _uv_sync }}

    uv run python .github/scripts/dst_nightly.py --matrix


# Run the whole nightly matrix locally and gate it (minutes at the default band)
dst-nightly-all seeds='65536':
    {{ _uv_sync }}

    rm -rf .nightly && mkdir -p .nightly

    # One `--matrix` call feeds both the fan-out and the expectation, for the same reason CI
    # derives both from one job output: two calls are two lists, free to disagree.
    cells="$(uv run python .github/scripts/dst_nightly.py --matrix | \
        python3 -c 'import json,sys; print(" ".join(c["cell"] for c in json.load(sys.stdin)))')"; \
    for cell in $cells; do \
        uv run python .github/scripts/dst_nightly.py \
            --cell "$cell" --seeds {{ seeds }} --out ".nightly/$cell.json" >/dev/null; \
    done; \
    uv run python .github/scripts/dst_nightly.py \
        --verdict .nightly --expect "$(echo $cells | tr ' ' ',')"


# Run the DST detection-time pilot campaign (writes pages/docs/dst/_generated/campaign_pilot.md)
dst-campaign:
    {{ _uv_sync }}

    PYTHONPATH=. uv run forze dst campaign tests.support.misuse:CORPUS \
        --controls tests.support.misuse:CONTROLS \
        --campaigns 100 --ceiling 2000 --fp-runs 400 --master-seed 0 \
        --out dst-campaigns.jsonl \
        --summary pages/docs/dst/_generated/campaign_pilot.md


# Run the full DST detection-time protocol: N=300 campaigns + W3 analysis + charts (~10 min)
dst-campaign-full:
    {{ _uv_sync }}

    PYTHONPATH=. uv run forze dst campaign tests.support.misuse:CORPUS \
        --controls tests.support.misuse:CONTROLS \
        --campaigns 300 --ceiling 2000 --fp-runs 400 --master-seed 0 \
        --out dst-campaigns-full.jsonl \
        --summary pages/docs/dst/_generated/campaign_full.md
    PYTHONPATH=. uv run python .github/scripts/analyze_campaign.py \
        dst-campaigns-full.jsonl \
        --summary pages/docs/dst/_generated/campaign_full.md
    PYTHONPATH=. uv run python .github/scripts/render_campaign_charts.py \
        dst-campaigns-full.jsonl \
        --out pages/docs/dst/_images


# Regenerate the DST fidelity matrix artifact (needs Docker; writes pages/docs/dst/_generated/)
dst-fidelity:
    {{ _uv_sync }}

    FORZE_FIDELITY_OUT=pages/docs/dst/_generated uv run pytest \
        "tests/integration/test_forze_postgres/test_pg_isolation_conformance.py::TestPostgresFidelityMatrix" \
        "tests/integration/test_forze_mongo/test_mongo_isolation_conformance.py::TestMongoFidelityMatrix" \
        -q
    uv run python .github/scripts/render_fidelity.py \
        pages/docs/dst/_generated/fidelity_postgres.json \
        pages/docs/dst/_generated/fidelity_mongo.json \
        --out pages/docs/dst/_generated/fidelity.md


# Regenerate the corpus bug-transfer artifact (needs Docker; writes pages/docs/dst/_generated/)
dst-transfer:
    {{ _uv_sync }}

    FORZE_FIDELITY_OUT=pages/docs/dst/_generated uv run pytest \
        "tests/integration/test_forze_postgres/test_pg_misuse_transfer.py" \
        -q
    PYTHONPATH=. uv run python .github/scripts/render_transfer.py \
        pages/docs/dst/_generated/transfer_postgres.json \
        --out pages/docs/dst/_generated/transfer.md
    PYTHONPATH=. uv run python .github/scripts/analyze_transfer_predictor.py \
        --fidelity pages/docs/dst/_generated/fidelity_postgres.json \
        --transfer pages/docs/dst/_generated/transfer_postgres.json \
        --out pages/docs/dst/_generated/predictor.md


# Run all quality checks
[arg("strict", long, short="s", value="true", help="Enable strict mode (fail on error in any check)")]
quality strict="false":
    {{ _uv_sync }}

    just _uv_cmd "Linting" {{ strict }} ruff check "src"
    just _uv_cmd "Formatting" {{ strict }} ruff format --check "src"
    just _uv_cmd "Types" {{ strict }} mypy "src"
    just _uv_cmd "Imports" {{ strict }} lint-imports
    just _uv_cmd "Determinism" {{ strict }} pytest "tests/unit/test_determinism_guard.py" -q
    just _uv_cmd "Sealed sort" {{ strict }} pytest "tests/unit/test_sealed_sort_guard.py" -q
    just _uv_cmd "Mock coverage" {{ strict }} pytest "tests/unit/test_mock_coverage_guard.py" -q
    just _uv_cmd "Conformance" {{ strict }} python .github/scripts/conformance_manifest.py --collect
    just _uv_cmd "CI matrix" {{ strict }} pytest "tests/unit/test_ci_matrix_guard.py" -q
    just _uv_cmd "Gitmoji excerpt" {{ strict }} pytest "tests/unit/test_gitmoji_excerpt_guard.py" -q
    just _uv_cmd "Docs floors" {{ strict }} python .github/scripts/docs_floors.py
    just _uv_cmd "Skills corpus" {{ strict }} python -m tools.skills_check
    just _uv_cmd "Dead code" {{ strict }} vulture
    just _uv_cmd "Dependencies" {{ strict }} deptry .
    just _uv_cmd "Security" {{ strict }} bandit -c pyproject.toml -r "src"
    just _uv_cmd "Workflows" {{ strict }} zizmor --collect=default .github/
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

# ----------------------- #
# Coverage floors

# Enforce per-package coverage floors on existing combined coverage data (.coverage)
coverage-floors-check:
    {{ _uv_sync }}

    uv run coverage json --fail-under=0 -o coverage.json
    uv run python .github/scripts/coverage_floors.py coverage.json

# Run the full suite with coverage (unit + integration; Docker), then enforce the floors
coverage-floors *args='':
    just test {{ args }} --cov=src --cov-report=
    just coverage-floors-check
