# The altitude model in full

The five bands, the Diátaxis compass that places a page, and the
range each archetype is allowed.

Every page descends through these five bands, **in order**. A page may start
below band 1 and may stop early, but it must never jump back up or skip a
required band on the way down. One sanctioned exception: a tutorial *closes*
with a band-2 retro ("What you just did") after its descent is complete — a
deliberate return to concepts once the doing is done, allowed only as a
closing section (see the tutorial contract).

1. **Orientation** — why this exists, what problem it removes. Plain language, no
   API names. The reader learns whether they're on the right page.
2. **Mental model** — the shape of the thing. One diagram, one analogy, or two to
   three sentences. Concept nouns only.
3. **The shape in code** — the smallest *real* anchor. One canonical snippet,
   ideally an include from the examples source. The reader sees it concretely.
4. **Mechanics** — the moving parts: the API surface, the table of
   methods/options, how it actually runs.
5. **Edge & operational detail** — caveats, failure modes, tuning, exhaustive
   lookup. The reader resolves a specific question.

## Diátaxis fit — the compass

Diátaxis generates the four archetypes from two questions. This is the Diátaxis
compass; use it to place new content *and* to course-correct an existing page
that feels off:

1. **Action or cognition?** — does the content inform *doing* (practical steps)
   or *knowing* (facts, theory)?
2. **Acquisition or application?** — is the reader *studying* (building skill)
   or *working* (applying skill they have)?

| Informs | Serves | Archetype |
|---|---|---|
| action | acquisition (study) | **Tutorial** — learning-oriented |
| action | application (work) | **How-to** — task-oriented |
| cognition | application (work) | **Reference** — information-oriented |
| cognition | acquisition (study) | **Explanation** — understanding-oriented |

One page = one job — never mix two jobs on a page. A draft that answers two of
these questions is two pages, cross-linked.

The single most common conflation in software docs is tutorial vs how-to. A
tutorial is a *lesson*: the author owns the reader's success, controls the
environment, walks one reliable path. A how-to guide is *directions*: the
reader is competent, owns the outcome, works in the real world — so the guide
may fork and branch. A "tutorial" that assumes competence and branches is a
mislabeled how-to; fix the label or the page.

## Altitude range per archetype

| Archetype | Enters at | Floors at | Must NOT |
|---|---|---|---|
| Tutorial | 1 | 3 (touch 4 once) | bottom out at 5; enumerate edge cases |
| Explanation | 1 | 4 | dump raw lookup; dip to 5 except its one subject |
| Deep explanation | 1 | 4, dips to 5 on its subject | become a reference dump or a how-to |
| How-to | 1 (**one line**) | 4 | rebuild the mental model (band 2) — link to it instead |
| Reference-practical | 1 (**one line**) | 4 | go conceptual (band 2) |
| Reference (lookup) | band-1 one-liner, then 4 | 5 | narrative progression; "where next" footers |

"Imbalanced" failures are now nameable: a page that is all narrative has no band 3
anchor; a page that opens on code skipped bands 1–2; a how-to that re-teaches
concepts dipped into band 2 it should have linked instead.
