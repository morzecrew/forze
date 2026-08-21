# Components

Different site generators support different components, and a marker that renders
in one setup leaks as literal text in another. Before using a component:

- Confirm it renders in the project's generator (admonitions, tabs, grid cards,
  buttons, icons, light/dark diagrams, tooltips, snippet includes).
- If a feature is a no-op in the setup (e.g. an unsupported code-annotation
  syntax), use a supported substitute (an inline comment plus a following
  admonition or bullet) instead of leaking markup.

Capture the verified component list once for the repo so later pages don't
rediscover it.
