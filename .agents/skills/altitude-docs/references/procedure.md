# The procedure, and how to verify what it produced

## The procedure

1. **Identify the archetype** from the target directory (or, for new content,
   from the reader's need — see Diátaxis fit below). This fixes the altitude
   range, the page contract, and the handoff style.
2. **Set the altitude range** for that archetype (the table below). Note the
   entry band and the floor.
3. **Apply the page contract** — opening job, section skeleton, floor, handoff,
   allowed components, code policy.
4. **Apply the consistency layer** — voice, opening, handoff, component
   discipline, code, diagrams, accuracy. These ride on top of every contract.
5. **Run the ship rubric.** A page ships only when all ten checks pass.
6. **Verify the build** — clean build with no issues (see Build & verify).

## Build & verify

- **Render diagrams first.** If the project uses diagrams-as-code, render sources
  to their output location before referencing them, and reference them with the
  project's path/theme convention.
- **Preview locally** with the project's serve command while iterating.
- **Authoritative build check:** run the project's clean/full build (clearing any
  cache) and expect a zero-issue result. Warm/incremental builds often emit
  spurious, fluctuating warnings (e.g. transient "page does not exist" counts) —
  treat those as noise; only a clean build's result is real.
- **Snippet includes:** when the tooling supports pulling code from external
  files, prefer that over inline code, and keep include paths valid (a bad path
  should fail the build). Mark regions in the source file and reference them from
  a fenced block.
