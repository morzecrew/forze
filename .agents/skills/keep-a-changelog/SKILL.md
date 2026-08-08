---
name: keep-a-changelog
description: Maintain CHANGELOG.md in Keep a Changelog 1.1.0 format — sort changes into Added/Changed/Deprecated/Removed/Fixed/Security, keep the Unreleased section current, cut version sections, and handle breaking changes, reverts, and yanked releases. Use when asked to "update the changelog", "add to the changelog", "write release notes", "cut/prepare a release section", "bump the version", or when user-facing changes land that CHANGELOG.md should record.
---

# Keep a Changelog Assistant

Changelogs are for humans, not machines. A good changelog lets a user answer one question fast: "what does upgrading do to me?" This skill keeps `CHANGELOG.md` in [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/) format: the agent maintains `## [Unreleased]` and, when explicitly asked, prepares version sections. The human decides when and how to actually cut, tag, and publish a release.

## Use this skill when

- The user asks to update or maintain `CHANGELOG.md`, or says "update the changelog", "add to changelog"
- Landed changes are user-facing and `CHANGELOG.md` should record them
- The user wants changes categorized into `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`
- The user asks to turn accumulated `Unreleased` notes into a versioned section ("cut a release", "prepare 1.2.0")

## Do not use this skill when

- There is no `CHANGELOG.md`, or it deliberately follows a different format (e.g. auto-generated release notes)
- The changes are purely internal and invisible to users — nothing to record
- The user wants the release itself performed (tagging, publishing) — that stays with the human
- The user wants commit messages or PR titles — use `gitmoji-conventional`

## The Keep a Changelog spec

These rules come from the spec itself and apply in any repository.

### Structure

- `## [Unreleased]` at the top — tracks upcoming changes so users can see what's coming, and so release notes are already written when a release is cut
- One `## [X.Y.Z] - YYYY-MM-DD` section per released version, latest first; every version gets an entry
- Dates in ISO 8601 (`YYYY-MM-DD`) — unambiguous across locales
- Link references at the bottom make version headings linkable, conventionally pointing at compare URLs:

  ```text
  [unreleased]: https://github.com/org/repo/compare/v1.1.0...HEAD
  [1.1.0]: https://github.com/org/repo/compare/v1.0.0...v1.1.0
  [1.0.0]: https://github.com/org/repo/releases/tag/v1.0.0
  ```

### Categories

The spec defines exactly six change types. Do not invent new ones — every user-facing change fits one of these:

| Category | Use for |
|---|---|
| `Added` | new features |
| `Changed` | changes in existing functionality |
| `Deprecated` | soon-to-be removed features |
| `Removed` | now removed features |
| `Fixed` | any bug fixes |
| `Security` | in case of vulnerabilities |

Omit empty categories in released versions. Avoid duplicating one change across categories — pick the dominant effect (a rewrite that also fixes a bug is `Changed` if the rewrite is the story, `Fixed` if the fix is).

### Breaking changes

Keep a Changelog has no "Breaking" category. Record a breaking change under its natural category (`Changed` for altered behavior, `Removed` for deleted features) and state the break explicitly in the entry so a reader can't miss it:

```text
### Changed

- Authentication endpoints now require OAuth2; API-key access no longer works.
```

Every commit marked breaking (`!` / `BREAKING CHANGE:` footer, see `gitmoji-conventional`) must produce such an entry, and per SemVer implies a MAJOR version for the next release.

### Reverts

- Revert of a change still sitting in `Unreleased`: delete the original entry. The changelog records net user-visible change, not git history — shipping "added X" and "removed X" in the same release is noise.
- Revert of a change from an already-released version: add a new entry (usually `Fixed` if the revert cures a regression, otherwise `Changed`/`Removed`) that says what behavior is restored.

### Yanked releases

A version pulled for a serious bug or security issue keeps its section, tagged:

```text
## [0.0.5] - 2014-12-13 [YANKED]
```

Never delete a released version's section — users on that version still need its history.

## What belongs in the changelog

Only changes meaningful to consumers of the project. The test: does this affect how users install, import, use, or should trust the software? If not, leave it out even when technically substantial.

Typically in: public API changes, new or changed behavior, packaging/installation changes, deprecations, security fixes.

Typically out: test changes, CI/CD and workflow updates, internal tooling and agent skills, docs-only changes, formatting/lint-only changes, refactors with no observable behavior impact, trivial renames.

Read the product-code boundary from the repository's own layout — wherever the shipped code lives (`src/`, `cmd/` + `internal/`, `lib/`, a package directory) — and treat "user-relevant" as changes inside it: public APIs and commands, domain primitives, contracts and schemas, behaviors, plus packaging changes. Never assume one layout's boundary in another repository.

## Entry style

- **Self-contained.** Each entry stands alone — no references to other entries, commit hashes, or context the reader doesn't have.
- **Outcome-oriented.** Say what changed for the user, not how it was implemented. "Requests retry automatically on timeout", not "refactored HttpClient to wrap RetryPolicy".
- **Neutral and compact.** No marketing language.

### House rules (this repository)

These are deliberate local conventions, not part of the spec. In another repository, follow that repository's existing formatting instead.

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

## Checking the file

`scripts/validate_changelog.py` settles the mechanical rules — Unreleased present, heading and date format, latest-first ordering, the six categories, duplicates, and link references resolving both ways (skipped when the file uses none):

```bash
python3 scripts/validate_changelog.py CHANGELOG.md                 # spec only
python3 scripts/validate_changelog.py --house-rules CHANGELOG.md   # + the local conventions above
```

(Paths relative to this skill's directory; from a repository root the script is at `skills/keep-a-changelog/scripts/validate_changelog.py`.) Run it after editing, and before cutting a version. It never edits — and it cannot judge whether an entry is user-relevant, outcome-oriented, or true, which is the part that matters most.

## Workflow A — update `Unreleased`

1. Extract user-facing changes from the user's summary, commits, PR descriptions, or diffs.
2. Drop everything that fails the "what belongs" test above.
3. Place each survivor under the best of the six categories in `## [Unreleased]`, applying the entry style and any house rules.
4. Output a diff or the updated `## [Unreleased]` block.

## Workflow B — cut a version section

Only when explicitly asked to convert `Unreleased` into a version:

1. Insert `## [X.Y.Z] - YYYY-MM-DD` directly under `## [Unreleased]`, using the target version and today's date in the user's timezone. Sanity-check the version against the content: breaking entries imply MAJOR, `Added` implies at least MINOR.
2. Move the `Unreleased` content into it, keeping category headings and omitting empty categories.
3. Reset `## [Unreleased]` to its empty state (in this repository: placeholder `- ...` under each category).
4. Leave the bottom reference links alone unless asked; note to the user that `[unreleased]` and the new version link should be updated.
5. Output the edits as a diff or updated blocks, plus any assumptions, and a reminder that tagging and publishing remain the human's job.

## Related skills

- `gitmoji-conventional` — commit messages and PR titles; its breaking-change markers (`💥`, `!`, `BREAKING CHANGE:`) are the signal that a changelog entry must call out a break.
