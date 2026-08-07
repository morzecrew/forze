# RFC 0027 — Documentation completeness program

- **Status:** 📝 Draft — a scheduled program, not a design fork. Phases and exit criteria are the deliverable; every phase closes against a number that already exists.
- **Scope:** Bring `pages/docs` and `skills/` from "gated for presence" to "complete and navigable", against the standard that already governs page construction. Execution only: this RFC decides *what gets written, in what order, and when it is done* — never *how a page is built* (`altitude-docs`) nor *how doc code stays current* ([RFC 0026](0026-executable-documentation.md)).
- **Related:** [`docs_floors.py`](../.github/scripts/docs_floors.py) — the gate this program shrinks; `.agents/skills/altitude-docs/SKILL.md` — the 298-line page standard, unchanged by this RFC; [RFC 0026](0026-executable-documentation.md) — code correctness, deliberately carved out; [RFC 0008](0008-docs-language-switcher-ru.md) — translation, downstream of this.
- **Origin:** Six framework audits recorded documentation debt as a recurring finding. The docs-floors gate converted the *recurring* half into a build failure, so new debt cannot accrue silently. What it did not do is discharge the standing balance: 51 declared exemptions, 26 of 37 contract planes with no reference page, and a reference layer whose largest single gap is one missing table column. This RFC is the schedule for that balance.

---

## 1. Why a program and not a design

The instinct on reading "the docs are hard to read" is to redesign something. Three
measurements say otherwise, and they are the reason this RFC has no decision log:

- **The page standard exists and works.** `altitude-docs` specifies the altitude model,
  Diátaxis page contracts, a consistency layer and a ship rubric. Pages that fall short of
  it fall short of a *known* bar. Nothing needs deciding; pages need writing.
- **The largest gap is a column.** 29 of 51 exempt symbols are dep keys, and
  `reference/contracts.md` is already their index — 26 rows of capability → spec →
  accessor. It lacks a **Dep key** column. This is the single highest-leverage edit in the
  corpus and it is an afternoon.
- **The one genuine fork was extracted.** Whether doc code should be executable, and how,
  is RFC 0057. Leaving it inside a "master the docs" RFC would have buried a real design
  question under a work plan.

What remains is a backlog with an existing gate to measure it against. The value of writing
it down is that the balance stops being folklore: each phase names its number, and the
number is one `just docs-check` away from being checked.

## 2. The measured balance

| Debt | Now | Source |
| --- | --- | --- |
| Contract symbols with no doc mention | 51 exempt (82/133 documented) | `docs_floors.py` |
| — of which dep keys awaiting one index column | 29 | `port-plane-undocumented` group |
| — identity lifecycle ports | 14 | `identity-plane-undocumented` group |
| — crypto keys bound by `CryptoDepsModule` | 3 | `crypto-internals-undocumented` group |
| — structural base types (declined, see §5) | 5 | `structural-base-types` group |
| Contract planes with a reference page | 11 of 37 | `pages/docs/reference/contracts/` |
| Agent Skills verified against the API | 0 of 21 | nothing checks them |

## 3. Phases

Each phase is independently shippable and closes against a number.

**P1 — The dep-key column.** Add a `Dep key` column to `reference/contracts.md`, covering
its 26 capability rows, and delete the `port-plane-undocumented` and
`crypto-internals-undocumented` exemption groups. The gate refuses a redundant exemption,
so the deletion is forced rather than remembered.
*Exit: 51 → 19 exemptions; the wiring-facing half of every documented plane is reachable.*

**P2 — Identity port reference.** The identity plane splits across many small lifecycle
ports and is taught only through presets and recipes, which serves someone adopting it and
fails someone replacing one port. One reference page, same shape as the existing eleven.
*Exit: 19 → 5 exemptions; only the declined group remains.*

**P3 — Plane reference parity.** 26 of 37 contract subpackages have no reference page.
Triage first: some are internal (`deps`, `base`, `resolution`) and want no page, which is a
judgement to record in the same exemption grammar rather than leave implicit. Write pages
for the rest, and extend `docs_floors.py` with a plane-level check so the ratio is gated
like the symbol list is.
*Exit: every contract plane either has a reference page or a reasoned exemption.*

**P4 — Skills parity.** 21 shipped Agent Skills, consumed by app authors through
`npx skills add`, verified by nothing. Three were stale in a single session and one taught
a call that had become a `TypeError`. Bring them under a gate — the mechanism is RFC 0026's
open question 3, so this phase **blocks on 0026** and is listed here only so the exposure
is not recorded in two places and owned in neither.
*Exit: a skill teaching a signature that no longer exists fails CI.*

**P5 — Readability pass.** Only after P1–P3, because "hard to read" is partly "the thing I
needed was not there". Apply the `altitude-docs` polish mode to the outliers the corpus
already shows: `credential-rotation.md` (2,721 words), `reference/fastapi-routes.md`
(2,437), `identity-tenancy-enc/encryption.md` (2,397) — pages long enough that they are
probably carrying two altitudes at once.
*Exit: no page over ~2,000 words that has not been through polish mode or justified its
length.*

## 4. Ordering rationale

P1 before everything because it is the cheapest and it changes what "51 exemptions" means —
a reviewer looking at the table after P1 sees a real backlog rather than a bookkeeping
artifact. P5 last because polishing prose around a missing reference page is how a corpus
gets longer without getting clearer. P4 sits where it does because it is the highest
*exposure* (agents consume skills and cannot tell the API is wrong) but is blocked on a
design question that is not this RFC's to answer.

## 5. Non-goals

- **The five structural base types stay exempt.** `BaseSpec` and its four siblings are
  parts of documented surfaces, not vocabulary an application uses. Documenting them adds a
  name to learn without adding a decision to make — the one place the gate's bar is worth
  declining rather than meeting, and it should stay declined explicitly.
- **No autodoc.** The curated reference layer is deliberate and now gated for completeness;
  generating it from docstrings would trade the thing the docs are good at for the thing
  the gate already covers.
- **No nav restructure.** The six-section split is Diátaxis-shaped and the measurements
  point at missing pages, not at misplaced ones. Revisit only if P1–P3 land and the corpus
  still reads badly — which would be evidence, rather than the assumption it is today.
- **No prose standard.** `altitude-docs` owns it.

## 6. Decision log

*(intentionally empty — this RFC schedules work against decisions already made elsewhere.
A choice that needs arguing belongs in RFC 0026 or in the altitude-docs standard, not here.)*
