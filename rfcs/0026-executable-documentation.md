# RFC 0026 — Executable documentation

- **Status:** 📝 Draft — problem measured, design open.
- **Scope:** The 271 inline Python blocks in `pages/docs` that nothing executes. Make a doc snippet that no longer compiles against the current API a build failure, the way `examples/` already are and the way `docs_floors.py` now makes a missing symbol one. Not about prose, structure, or style — those have an owner already.
- **Related:** [`docs_floors.py`](../.github/scripts/docs_floors.py) (presence: the symbol is mentioned somewhere), `examples/` + `tests/unit/test_examples/` (the precedent: runnable examples are test-backed "so examples can't silently rot"), the `--8<--` snippet include that already pulls 81 blocks out of `examples/`, and the `altitude-docs` skill (which governs how a page is built and explicitly *not* what is true).
- **Origin:** In one session two published pages — `integrations/fastapi.md` and `data-events/realtime.md` — began teaching `build_realtime_mailbox(ctx)`, a call that had become a `TypeError` earlier the same session. Both were found by grepping for the symbol by hand. Nothing in `just quality`, the docs build, or CI failed. The same change broke a shipped Agent Skill, which reaches app authors through `npx skills add`.

---

## 1. The measurement

| | count |
| --- | --- |
| Python blocks pulled from `examples/` via `--8<--` (executed by `tests/unit/test_examples/`) | 81 |
| Python blocks written inline (executed by nothing) | **271** |
| Pages carrying at least one inline block | 87 of 132 |

`integrations/fastapi.md` — one of the two pages that broke — contains **zero** snippet
includes. Every line of Python on it is prose that happens to look like code.

So 77% of the Python a reader copies out of these docs has no relationship to the codebase
beyond having been correct when it was typed. That is the same class of defect the
conformance work spent six editions on, in a different medium: a claim nothing checks.

## 2. Why this is the RFC, and the rest is not

Three things get conflated under "master the documentation". Only one is a design question.

**Prose quality and structure — has an owner.** `.agents/skills/altitude-docs/SKILL.md` is
a 298-line standard: the altitude model, Diátaxis page contracts, a shared consistency
layer, a ship rubric. It is applied, and it explicitly governs "*how* a page is built, not
*what is true*". An RFC restating it would duplicate a working standard; pages that fall
short of it are a **backlog**, and you schedule a backlog rather than RFC it.

**Symbol coverage — measured, and mostly one column.** `docs_floors.py` reports 82/133
contract symbols documented with 51 declared exempt, which reads alarming. But 29 of those
51 are dep keys in a single group, and `reference/contracts.md` is *already* the capability
index they belong in — 26 rows of capability → spec → accessor, covering 20 distinct
accessors. It is missing a **Dep key** column. That is an afternoon against an index that
exists, not a design decision.

**Code correctness — nothing owns it.** No standard covers it, no gate detects it, and the
failure is silent by construction: a wrong snippet renders exactly like a right one. This
is the gap.

## 3. What makes it genuinely open

There is no obvious answer, which is why it needs an RFC rather than a ticket.

**Not every snippet can execute.** Docs legitimately contain fragments — a three-line
excerpt showing one keyword argument, a `...` elision, a wiring block that needs a live
Postgres. A design that demands every block run will be worked around within a month
(blocks fenced as `text`, or the check disabled), which is worse than no design.

**"Compiles" and "runs" are different bars, with very different costs.** Parsing a block
and resolving the symbols it imports would have caught the `build_realtime_mailbox(ctx)`
regression — it is a signature change — at roughly the cost of an AST walk. Actually
*running* it needs a context, deps, and often a container. The cheap bar may catch most of
the value; that is a claim this RFC should test against the 271 blocks before committing.

**The `examples/` path already exists and is stronger.** Anything moved into `examples/`
and included with `--8<--` is executed by a real test. The honest question may not be "how
do we verify inline blocks" but "which of the 271 should stop being inline" — with
verification as the forcing function rather than the feature. That reframing would make
this a curation project with a gate, not a new mechanism.

## 4. Candidate designs (not yet chosen)

1. **Compile-check every fenced `python` block.** Parse with `ast`, resolve imports against
   the installed packages, flag unknown symbols and — the case that bit — calls whose
   keyword arguments do not match the signature. Cheap, no execution, no containers.
   Catches signature and rename drift; blind to behaviour.
2. **Opt-in execution markers.** A block tagged `python exec` is run in a mock-backed
   context; untagged blocks get the compile check only. Honest about fragments, but the
   tag is exactly the thing an author under time pressure omits.
3. **Migrate to `examples/`, gate the residue.** Move every block that *can* be a runnable
   example into `examples/`, include it, and let the gate cap how many inline blocks a page
   may retain (a floor that ratchets down, like the exemption table).
4. **Do nothing mechanical; put snippets under the review rubric.** Rejected on the
   session's own evidence — review is what did not catch it.

## 5. Open questions

1. What fraction of the 271 blocks would pass a compile check today? Until that is
   measured, the cost of design 1 is unknown, and it may be that most failures are
   deliberate fragments rather than drift.
2. Do fragments get a first-class fence (`python fragment`) so "unverified" is declared
   rather than inferred from a check that skipped it?
3. Do the shipped Agent Skills (`skills/*/SKILL.md`) come under the same gate? They broke
   in the same session, they ship to app authors via `npx`, and nothing checks them at all
   — arguably a sharper exposure than the docs, since a skill is consumed by an agent that
   will not notice the API is wrong.
4. Does this replace the exemption-table shape or reuse it? A per-page inline-block ceiling
   that can only decrease is the same ratchet, applied to a different unit.

## 6. Non-goals

- Rewriting prose, restructuring the nav, or changing archetype boundaries.
- Generating an API reference from docstrings. The curated reference layer is deliberate,
  and `docs_floors.py` now gates its completeness; adding autodoc would trade the thing the
  docs are good at for the thing the gate already covers.
- Verifying that a snippet is *good* — only that it is *current*.

## 7. Decision log

*(empty — this RFC records a measured problem, not yet a resolution)*
