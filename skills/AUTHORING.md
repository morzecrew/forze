# Authoring Forze Agent Skills

This file is for **maintainers** of `skills/` in the Forze repository. It is not published as an installable skill, and it must stay at `skills/` rather than inside `forze-skills/` — the installer copies a skill directory recursively, so anything under it ships into every consumer's `.claude/skills/`.

## Shape

One published skill, `skills/forze-skills/`, whose `SKILL.md` is a **routing index** over the reference files in `skills/forze-skills/references/`. The index is navigation; the references are the material.

```
skills/
  AUTHORING.md          maintainer rules (this file, not published)
  README.md             install instructions (not published)
  forze-skills/
    SKILL.md            frontmatter + mental model + routing table + index
    references/*.md     one job per file
```

Per-skill install is gone: it never worked. Installing one directory left every cross-link dangling, which is what the consolidation fixed.

## Audience

Skills target engineers building **applications** that depend on Forze (`forze`, `forze_postgres`, `forze_inngest`, etc.) from PyPI — not contributors changing the Forze monorepo.

| Teach in skills | Do not teach in skills |
|-----------------|------------------------|
| Logical specs, handlers, ports, wiring, integration extras | Moving code between `forze` and integration packages |
| `DepsRegistry`, built-in `*DepsModule`, `forze_mock` tests | Import-linter contracts, CHANGELOG, CI, `AGENTS.md` workflow |
| Custom `DepsModule` in **your app** (advanced) | Reading `src/forze_*` to implement framework adapters |

Framework contributors should use [`AGENTS.md`](../AGENTS.md), [canonical docs](https://morzecrew.github.io/forze/latest/), and `.claude/skills/`.

## Vocabulary

| Prefer | Avoid |
|--------|--------|
| logical spec / application spec | kernel spec (unless quoting API names) |
| integration package (`forze_inngest`) | adapter package vs core contracts |
| your application / service | this repository |
| shipped `forze_*` package | repo layout under `src/` |

## Splitting rules

A reference file is **one job** — something a reader came to do ("wire the runtime", "write a query", "rotate a key").

1. **Never split a procedure.** If steps 1–5 must be followed in order, they are one file however long.
2. **60–250 lines.** Under 60 usually means it belongs with its neighbour; over 250 means it is two jobs. A file below the floor needs a reason worth writing down — `architecture` is short because it is a primer, not because it was cut badly.
3. **A file only ever read together with another is not a separate file.** This is the guard against over-atomisation.
4. **A reference must stand alone.** No "the snippet above" across a file boundary — repeat the import or link to the file that has it.

## The index is the product

The routing table is where this structure's risk lives, not the file split. An agent that reads one reference when the task needed three writes confidently incomplete wiring, so:

- The table is keyed by **task**, and a row names **every** reference needed to finish it.
- `SKILL.md` states the read-more-than-one norm explicitly.
- Every reference appears in the index, and every index row resolves to a file. `just skills-check` enforces both directions.

## Links

- **Published docs:** `https://morzecrew.github.io/forze/latest/...`. Docs are **versioned** with mike — the bare `.../forze/<page>/` form (no version segment) **404s**; always include `latest` and a trailing slash.
- **The versioned-docs note lives in `SKILL.md` once**, not in every reference.
- **Between references:** plain relative links, e.g. `[query DSL](query-dsl.md)`.
- **Never** link anywhere outside `skills/` — not `../../src/`, `../../tests/`, `../../pages/` or `../../examples/`. Installed skills are copied outside the Forze repo, so every such path breaks there. Naming a repo path in prose is fine when qualified as such ("in the Forze repo"); making it a link is not.

## Reference file structure

1. `# Title` — the job, not the topic.
2. Body: the procedure, minimal examples, gotchas.
3. **Anti-patterns** — only mistakes an **app team** can make. Routed by subject: an anti-pattern about `route=spec.name` belongs in `deps-resolution.md`, not in a corpus-wide file. A reference with no mistake of its own has no such section, and that is fine.
4. **Reference** — published doc URLs and sibling references.

**Include:** adapter imports in handlers; missing `route=spec.name`; unfrozen registry; binding identity inside handlers; wrong port for the integration.

**Exclude:** "keep X in `forze_inngest` not core contracts"; pointers to Forze `tests/`; "no core contract changes" (reframe as "use `TokenVerifierPort` from the OIDC package").

## Changing the file set

The reference map is owned by RFC 0041 §6 and is **locked, not frozen**. Adding or removing a reference file is an explicit amendment to that section, landing in the same change that creates or deletes the file. Nothing else may introduce one — the parity gate makes this enforceable rather than aspirational, since an unindexed file fails the build.

## Retired skill names

Do not re-create these. The 21 published skills were merged into `forze-skills` (breaking, one hard cut):

```
forze-analytics            forze-auth-tenancy-secrets  forze-custom-deps
forze-deps-consumption     forze-documents-search      forze-domain-aggregates
forze-encryption-kms       forze-fastapi-interface     forze-framework-usage
forze-graph-contracts      forze-http-outbound         forze-inference
forze-inngest-durable-functions                        forze-messaging-streaming
forze-object-storage       forze-observability-errors  forze-realtime
forze-resilience-deadlines forze-specs-infrastructure  forze-temporal-workflows
forze-wiring
```

Earlier retirements, merged before the consolidation: `forze-storage-s3` / `forze-storage-gcs` → `forze-object-storage`; `forze-analytics-bigquery` / `forze-analytics-clickhouse` → `forze-analytics`; `forze-deps-modules` (never published).

## Coverage doctrine

Every shipped package, and every boundary an extra draws inside one, is a **census unit** carrying a doctrine in [`tools/skills_check/coverage.toml`](../tools/skills_check/coverage.toml). A unit with no doctrine fails the build — so adding `src/forze_opensearch/` to the wheel targets, or adding a `kms-azure` extra that subdivides a package already covered, is a decision someone writes down rather than a gap nobody notices.

| | Requires |
|---|---|
| **D1** worked example | an importable block showing the deps module and its config type |
| **D2** import anchor | a resolved import plus the config type, for a unit reached identically to a covered sibling |
| **D3** out of scope | a written rationale |
| **D4** deferred | a rationale *and* a trigger that would move it to D1 or D2 |

D1 and D2 differ in how much surrounding material is expected, **never in whether the import is verified** — both resolve under the same gate. Naming a symbol in prose, in a table, or in a frontmatter description counts for nothing: the census scores consumption, not declaration, because a corpus can describe a backend in detail while no gate can see a single one of its claims.

## Adding or changing content

1. Edit the reference file that owns the job, or amend RFC 0041 §6 to add one.
2. Link integration topics to an existing page under `pages/docs/` (add a doc page if missing).
3. Update the routing table in [`forze-skills/SKILL.md`](forze-skills/SKILL.md) — both the index row and any task bundle the change affects.
4. Run `just skills-check`. It parses every python block, resolves every `forze*` symbol against the installed packages, and checks structure, index parity and link integrity — including the escape rule above, which used to be a grep a maintainer was asked to remember.

A python example that is deliberately not a whole module — and therefore cannot be parsed — is marked at its fence as ` ```python fragment `; the check fails a marked block that parses fine, so the marker cannot be used to opt a block out.

Published doc URLs are swept for liveness on a schedule rather than per change, because they break for reasons a given edit does not control (a page renamed in `pages/`, a mike alias moved).
