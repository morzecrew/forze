---
name: keep-a-changelog
description: Maintain CHANGELOG.md in Keep a Changelog format by updating Unreleased and version sections with user-relevant changes.
---

# Keep a Changelog Assistant

This skill helps keep `CHANGELOG.md` accurate, current, and user-focused following **Keep a Changelog** format.

It is intended for maintaining changelog entries as the product evolves.  
The agent helps update `## [Unreleased]` and, when explicitly asked, prepare version sections in the changelog.  
The human remains responsible for deciding when and how to cut a release.

## Use this skill when

- The user explicitly asks to update or maintain `CHANGELOG.md`
- The user provides changes and wants them added to `## [Unreleased]`
- The repository changes imply that `CHANGELOG.md` should be updated
- The user wants help categorizing changes into `Added`, `Changed`, `Fixed`
- The user wants to turn accumulated `Unreleased` notes into a versioned changelog section

## Do not use this skill when

- There is no `CHANGELOG.md`
- `CHANGELOG.md` does not follow **Keep a Changelog** structure
- The requested changes are purely internal and not relevant to users
- The user is asking the agent to publish, tag, or perform the release itself

## Repository Conventions (MUST FOLLOW)

### Scope of Changelog Entries

The changelog must include **only user-relevant product changes**.

For this repository, that generally means:

- Changes to code inside `src/`
- Changes affecting public APIs
- New domain primitives, contracts, or behaviors
- Behavioral changes in existing modules
- Packaging changes that affect installation or runtime behavior

The changelog must NOT include:

- Test changes (`tests/`)
- CI/CD workflow updates
- GitHub Actions changes
- Agent skills or internal tooling
- Documentation-only changes
- Formatting or lint-only changes
- Refactors with no observable behavior impact
- Internal development tooling adjustments

If a change does not affect how users import, use, or install the library,
it should generally be excluded from the changelog.

### Notability Rule

Only include changes that are meaningful to library consumers.

Do not include:

- trivial renames
- internal restructuring
- minor refactors without API impact
- small code cleanups

### Changelog format

`CHANGELOG.md` must remain in this structure:

- `## [Unreleased]`
  - `### Added`
  - `### Changed`
  - `### Fixed`
- Version sections:
  - `## [X.Y.Z] - YYYY-MM-DD`
  - same categories inside as needed
- Reference links at the bottom exist (for example `[unreleased]: ...`, `[0.1.1]: ...`)

**Important:**  
Do not add or modify bottom reference links unless explicitly asked.

## Categorization rules

- **Added**: new public APIs, new primitives, new modules, new features
- **Changed**: behavior changes, notable usage changes, meaningful adjustments visible to users
- **Fixed**: bug fixes, packaging/metadata fixes, build/installation fixes

Additional rules:

- Avoid duplicates between categories
- Keep entries short, neutral, and user-focused
- Prefer outcome-oriented wording over implementation detail
- Exclude internal-only changes even if they are technically substantial

## Workflow A — Update `Unreleased`

When the user says "update changelog", "add to changelog", or provides a list of changes:

1. Extract notable user-facing changes from the user summary, commits, PR descriptions, or diffs.
2. Exclude non-user-facing and non-notable changes.
3. Place each remaining change under the best category in `## [Unreleased]`.
4. Preserve existing formatting and spacing.
5. Output:
   - a patch/diff, or
   - the updated `## [Unreleased]` block

## Workflow B — Create a version section in `CHANGELOG.md`

When the user explicitly asks to convert current changelog notes into a versioned section:

1. Use the target version `X.Y.Z`.
2. Use today's date in `YYYY-MM-DD` using the user's timezone.
3. Create a new section directly under `Unreleased`:

   `## [X.Y.Z] - YYYY-MM-DD`

4. Move content from `## [Unreleased]` into that new version section:
   - Keep the same category headings
   - Omit empty categories
5. Reset `## [Unreleased]` categories back to placeholder state (`- ...`)
6. Do **not** edit reference links at the bottom unless explicitly asked
7. Output the proposed changelog edits as a diff or updated blocks

## Output format (always)

When updating the changelog, always produce:

1. Proposed `CHANGELOG.md` changes (diff or updated sections)
2. Any assumptions or TBDs (if applicable)
3. A short note that release decisions and tagging are handled by the human, while the agent only helps keep `CHANGELOG.md` current
