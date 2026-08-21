# Keep a Changelog: the spec, the house rules, and the two workflows

Mechanical detail lifted out of `SKILL.md`. Most of it is enforced by
`scripts/validate_changelog.py`. Read this when editing the file; the skill body
carries the only part a script cannot settle.

Format: [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/).

## Structure

- `## [Unreleased]` at the top — tracks upcoming changes so users can see what's coming, and so release notes are already written when a release is cut
- One `## [X.Y.Z] - YYYY-MM-DD` section per released version, latest first; every version gets an entry
- Dates in ISO 8601 (`YYYY-MM-DD`) — unambiguous across locales
- Link references at the bottom make version headings linkable, conventionally pointing at compare URLs:

  ```text
  [unreleased]: https://github.com/org/repo/compare/v1.1.0...HEAD
  [1.1.0]: https://github.com/org/repo/compare/v1.0.0...v1.1.0
  [1.0.0]: https://github.com/org/repo/releases/tag/v1.0.0
  ```

## Categories

The spec defines exactly six. Do not invent new ones.

| Category | Use for |
|---|---|
| `Added` | new features |
| `Changed` | changes in existing functionality |
| `Deprecated` | soon-to-be removed features |
| `Removed` | now removed features |
| `Fixed` | any bug fixes |
| `Security` | in case of vulnerabilities |

Omit empty categories in released versions. Avoid duplicating one change across categories — pick the dominant effect (a rewrite that also fixes a bug is `Changed` if the rewrite is the story, `Fixed` if the fix is).

## Breaking changes

There is no "Breaking" category. Record the change under its natural category (`Changed` for altered behavior, `Removed` for deleted features) and state the break explicitly in the entry:

```text
### Changed

- Authentication endpoints now require OAuth2; API-key access no longer works.
```

## Reverts

- Revert of a change still sitting in `Unreleased`: delete the original entry. The changelog records net user-visible change, not git history — shipping "added X" and "removed X" in the same release is noise.
- Revert of a change from an already-released version: add a new entry (usually `Fixed` if the revert cures a regression, otherwise `Changed`/`Removed`) that says what behavior is restored.

## Yanked releases

A version pulled for a serious bug or security issue keeps its section, tagged:

```text
## [0.0.5] - 2014-12-13 [YANKED]
```

Never delete a released version's section — users on that version still need its history.

## Entry style

- **Self-contained.** Each entry stands alone — no references to other entries, commit hashes, or context the reader doesn't have.
- **Outcome-oriented.** Say what changed for the user, not how it was implemented. "Requests retry automatically on timeout", not "refactored HttpClient to wrap RetryPolicy".
- **Neutral and compact.** No marketing language.

## House rules (this repository)

Deliberate local conventions, not part of the spec. In another repository, follow that repository's existing formatting instead.

- **Blank line between bullets.** Never stack entries on adjacent lines:

  ```text
  # wrong
  - Something
  - Something 2

  # right
  - Something

  - Something 2
  ```

- **Length cap:** at most 3 sentences and 320 characters per entry.
- **Minimal inline code:** only for an essential identifier (a symbol, a flag); prefer prose.
- **No structural extras:** no tables, code blocks, migration steps, or upgrade guides inside entries — the changelog records what changed, not how to adapt.
- **Empty `Unreleased` categories** keep a `- ...` placeholder.
- **Do not add or modify the bottom reference links unless explicitly asked** — but when cutting a release, remind the user those links need updating.

## Workflow A — update `Unreleased`

1. Extract user-facing changes from the user's summary, commits, PR descriptions, or diffs.
2. Drop everything that fails the "which changes earn an entry" test in `SKILL.md`.
3. Place each survivor under the best of the six categories in `## [Unreleased]`, applying the entry style and any house rules.
4. Output a diff or the updated `## [Unreleased]` block.

## Workflow B — cut a version section

Only when explicitly asked to convert `Unreleased` into a version:

1. Insert `## [X.Y.Z] - YYYY-MM-DD` directly under `## [Unreleased]`, using the target version and today's date in the user's timezone. Sanity-check the version against the content: breaking entries imply MAJOR, `Added` implies at least MINOR.
2. Move the `Unreleased` content into it, keeping category headings and omitting empty categories.
3. Reset `## [Unreleased]` to its empty state (in this repository: placeholder `- ...` under each category).
4. Leave the bottom reference links alone unless asked; note to the user that `[unreleased]` and the new version link should be updated.
5. Output the edits as a diff or updated blocks, plus any assumptions, and a reminder that tagging and publishing remain the human's job.
