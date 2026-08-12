# RFC template

Copy the skeleton below into `NNNN-kebab-title.md`. Replace `<placeholders>`, delete the guidance blockquotes, and drop sections the RFC doesn't need (keep numbering contiguous). A minimal design-lock RFC keeps: header block, Design, Non-goals, Decisions.

---

```markdown
# RFC NNNN — <Title>

- **Status:** 📝 Draft
- **Scope:** <One dense paragraph: what this RFC covers and what it deliberately
  does not. New packages/modules touched, contract changes (or "no contract
  changes"), the boundary of the blast radius. This is the paragraph a reader
  uses to decide whether to read the rest.>
- **Related:** <Links to the code being touched (relative paths into the repo),
  other RFCs by number, prior art, external docs.>
- **Discussion:** <Optional — link to the PR / issue / thread where the design
  was or is being debated. Delete if none.>
- **Origin:** <Optional — where the design was ported or generalized from:
  a sibling project, a spike, a production incident. Delete if none.>

---

## 1. Summary

<What ships, in a few sentences. Write it last, once the design has settled.>

## 2. Motivation

<The problem, with evidence: measured numbers, real failure cases, links to the
code paths that hurt. If the motivation can't cite anything concrete, question
whether the RFC is needed.>

## 3. Current state

<What exists today, verified against the code — not from memory. Name the
files, ports, schemas involved. Surprising verified facts ("zero pyproject
edits needed — confirmed against line ~218") belong here; they save the
implementer a re-investigation.>

## 4. Goals / Non-goals

**Goals**

- <...>

**Non-goals**

- <Explicit exclusions with a reason each — "not X, that is Y's job". Non-goals
  prevent scope creep during implementation and re-litigation after.>

## 5. Design

<The core of the document. One subsection per workstream or component
(### 5.1, ### 5.2, …). Pin the design with real artifacts — signatures,
schemas, wire formats, config shapes — in code blocks; prose alone drifts.
State failure semantics explicitly (what raises, what is refused, what fails
closed). Where a decision was contested, keep one sentence on the rejected
alternative and why it lost; when the choice shaped the design, give it an
`### Alternatives considered` subsection that states each alternative's
trade-off, not just its rejection — that is what stops re-litigation.>

### 5.1 <Component / workstream>

<...>

## 6. Tests

<How the design is verified: new suites, conformance families, what parity is
asserted, what is explicitly not tested and why.>

## 7. Docs

<What documentation ships with the change, and any doc claims that must be
worded carefully (e.g. migration honesty, threat-model caveats).>

## 8. Out of scope

- <Each item: what is excluded, why, and what would change that ("named as the
  escape hatch, not built"). Different from Non-goals: these are adjacent
  things a reader might assume are included.>

## 9. Risks

- <Honest failure modes — technical risks, misreading risks ("X read as
  security theater"), operational risks. Each with the mitigation or the
  explicit acceptance.>

## 10. Unresolved questions

- <What must be settled before the design counts as locked, vs. what
  implementation is free to settle. Name each unknown and who/what resolves
  it. An empty section is a claim — only make it if true.>

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | <One decision per row, self-contained, with the load-bearing rationale compressed in — and, where a decision constrains the future non-obviously, its consequence ("locks us to X; changing later means Y"). This table is the contract: pickup should require reading it, not re-deriving it.> |
| 2 | `ASSUMED` | <Believed correct but not load-bearing. Execution may depart from it if building proves it wrong, and logs the departure.> |
| 3 | `OPEN` | <Deliberately delegated to implementation. Say what the question is and what would settle it; the executor decides and logs the decision. An absent row is not `OPEN` — it is silence, and silence gets filled by whoever arrives first.> |

## 12. Phasing

<What lands first, what is gated on what, what is demand-gated. "P1 = W1+W2"
style is fine if the workstreams are named in §5.>
```

---

## Notes on filling it in

- **An existing corpus outranks this template.** If the project's RFCs already
  use a different section set or numbering (say, Decisions at §10), match the
  corpus — `§NN` cross-references must stay unambiguous across the directory.
  This skeleton is for directories without an established shape.
- **Header ↔ filename sync:** the `NNNN` in the H1 must match the filename.
- **Status annotations:** the status line is allowed to carry nuance —
  "📝 Draft (execution-ready — one PR)", "📝 Draft — design locked, **not
  scheduled**", "✅ Complete — Shipped 2026-06-29: …; only P5 remains".
  Prefer an annotated true status over a clean false one.
- **Amendments over rewrites:** once an RFC leaves Draft, record divergences
  as dated notes in the status line or the affected section. The decision
  table is append-only — a reversed decision gets a new row citing the row it
  reverses, not an edit. History someone relied on stays readable.
- **Index row:** written at the same time as the RFC. The one-liner is a
  routing description, not a summary — one sentence saying which design this
  is, so a reader knows whether to open the file. What it decided belongs in
  §1 and §11, and the index never carries history. Target 200 characters,
  ceiling 300.
