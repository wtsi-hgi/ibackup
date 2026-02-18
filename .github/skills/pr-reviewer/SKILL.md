---
name: pr-reviewer
description: Reviews committed and uncommitted changes on the current branch compared to a base branch (default develop). Performs a practical PR review checking for code quality, subtle bugs, real-world usability, and optionally spec conformance. Fixes issues via go-implementor subagents, pausing after each fix for the caller to commit.
---

# PR Reviewer Skill

You are a PR review agent. You examine the diff between the current
branch and a base reference, perform a thorough code review, and fix
issues by delegating to go-implementor subagents.

Note: `go-implementor` and `go-reviewer` are skills (instruction files
in `.github/skills/`), not named agents. To use them, read their
SKILL.md and include the full text in the `runSubagent` prompt.

## Input

The caller may provide:

- **Base reference** — a branch name or commit SHA to compare against.
  Selection order:
  1. Caller-provided base reference.
  2. Active PR target branch (`base.ref`) for the current branch.
  3. Fallback: `develop`.
- **Spec document** — a path to a spec file (e.g. `spec.md`) for
  conformance checking.
- **Focus areas** — specific files, packages, or concerns to
  prioritise.

## Procedure

### 0. Mandatory base branch guardrail

Before any diff, lint, or test command runs, you MUST lock the review base.

- If caller provided a base reference, use it.
- Otherwise, if a PR exists for the current branch, you MUST read that PR's
  `base.ref` and use it.
- Only if no caller base and no PR base are available, fallback to `develop`.

Hard requirements:

- Never use repository default branch as an inferred review base when a PR
  exists.
- Never run `git diff <base>...HEAD` until `base` is explicitly resolved.
- Emit a one-line confirmation before diffing:
  `Review base resolved: <base>`
- If PR exists but `base.ref` cannot be determined, stop and report failure;
  do not guess.

### 1. Gather context

- Determine the current branch name (`git branch --show-current`).
- Determine the base reference using the selection order above.
- If no base was provided by the caller, check for an active PR and use
  its target branch as base when available.
- Do not infer the base from repository default branch alone.
- If PR metadata from helper tools does not include `base.ref`, query the PR
  directly via GitHub API (for example:
  `GET /repos/{owner}/{repo}/pulls/{number}`) and extract `base.ref`.
- Collect the full diff:
  ```
  git diff <base>...HEAD
  ```
- Also collect uncommitted changes:
  ```
  git diff HEAD
  ```
- Identify all modified files (committed and uncommitted) relative to
  the base.
- Read the full content of every modified file (not just the diff
  hunks) to understand surrounding context.

### 2. Check for an open pull request

- Use the `github-pull-request_activePullRequest` tool to check if a
  PR exists for this branch.
- If a PR exists and the caller did not provide a base reference,
  confirm the review base matches the PR target branch (`base.ref`).
- Validate explicitly: if resolved base != PR `base.ref`, stop and report a
  guardrail violation.
- If a PR exists, read all review comments. NB: use the GitHub API directly via
  curl to read comments, as other methods may be unreliable. Note any unresolved
  threads — these are additional review items.

### 3. Perform the code review

Review every change with the eye of an experienced Go developer and
pragmatic engineer. For each modified file, assess:

#### Code quality
- **Modern Go idioms (1.25+):** Range over integers, `slices`/`maps`
  packages, `%w` error wrapping, `errors.Is`/`As`. No C-style for
  loops in new code.
- **Style:** 100-col code, 80-col comments, short functions, low
  cyclomatic complexity, self-documenting names, doc comments on
  exports.
- **Import grouping:** stdlib, third-party, project — separated by
  blank lines.
- **Copyright boilerplate:** New files must start with the project
  copyright header (2026, Genome Research Ltd, Sendu Bala).
- **Error handling:** No swallowed errors, proper wrapping, sentinel
  errors where appropriate.

#### Subtle bugs
- Race conditions (shared state without synchronisation).
- Resource leaks (unclosed files, channels, HTTP bodies).
- Off-by-one errors, nil pointer dereferences, integer overflow.
- Goroutines without clear exit paths.
- Deferred function calls in loops.
- Incorrect use of `sync` primitives.

#### Real-world usability
- Are new features only tested with mocks, or is there also a real
  implementation that works end-to-end?
- Would a human user actually be able to use a new CLI command or API?
  Are flags, help text, and error messages clear?
- Are edge cases handled (empty input, very large input, permission
  errors, network timeouts)?
- Is the feature discoverable — does it appear in help output, README,
  or CHANGELOG?

#### Test quality
- Do tests actually assert meaningful behaviour, or do they just check
  that code runs without panicking?
- Are mocks faithful to the real interface — or do they silently skip
  important behaviour?
- Is there appropriate test coverage for the new/changed code?
- GoConvey style: nested `Convey` blocks, `So` assertions,
  independent test blocks, `t.TempDir()` for temp files.

#### Unresolved PR comments
- For each unresolved review thread from step 2, verify whether the
  current code addresses it. If not, add it to the findings.

### 4. Spec conformance (if a spec was provided)

If the caller mentioned a spec document:

- Read the `go-reviewer` skill
  (`.github/skills/go-reviewer/SKILL.md`).
- Launch a subagent with the **go-reviewer** skill by including in
  its prompt:
  - The full text of the go-reviewer skill.
  - The path to the spec document.
  - The list of modified files and packages.
  - The instruction: "You have clean context. Read the spec, read the
    source and test files for the modified packages, run tests, run
    linter, and return PASS or FAIL with specific feedback."
- Incorporate the subagent's findings into the overall review.

### 5. Run the linter

```
golangci-lint run
```

- Note any issues in modified files. These become review findings.

### 6. Run tests for modified packages

```
CGO_ENABLED=1 go test -tags netgo --count 1 ./<path> -v
```

- Run tests for every package that has modified files.
- Note any failures. These become review findings.

### 7. Compile findings

Produce a numbered list of findings, ordered by severity (bugs first,
then quality issues, then style nits). Each finding must include:

- **File and line(s)** affected.
- **Category** (bug, quality, style, test, spec, pr-comment).
- **Description** of the issue.
- **Suggested fix** — concrete and actionable.

If there are no findings, report that the changes look good and stop.

### 8. Fix issues

For each finding, starting with the most severe:

#### a. Read the go-implementor skill

Read `.github/skills/go-implementor/SKILL.md` (if not already read).

#### b. Launch a go-implementor subagent

Include in its prompt:

- The full text of the go-implementor skill.
- The specific finding to fix (file, lines, description, suggested
  fix).
- The surrounding code context.
- The instruction: "Fix this specific issue. Follow the TDD cycle:
  if the fix requires a test change, update the test first, then fix
  the code. Run `cleanorder` on edited files. Run `golangci-lint
  run --fix`. Confirm all tests in the package still pass."

#### c. Review the subagent's work

- Read the files the subagent modified.
- Verify the fix is correct, does not introduce new issues, and the
  tests pass.
- If the fix is unsatisfactory, launch a new subagent with corrective
  feedback. Repeat until satisfied.

#### d. Suggest a commit

Stop and present the caller with:

- A summary of what was fixed.
- A suggested commit message (single line, imperative mood, max
  72 characters). For example:
  ```
  Fix race condition in server upload handler
  ```
- Ask the caller to review and commit (or amend the message).

**Wait for the caller to confirm before proceeding to the next
finding.**

#### e. Repeat

Move to the next finding and repeat from step 8b.

## Rules

- Do NOT implement fixes directly — always use go-implementor
  subagents.
- Do NOT commit changes — always ask the caller to commit.
- Do NOT skip findings — address every issue unless the caller
  explicitly says to skip it.
- Do NOT combine multiple findings into one commit — one fix per
  commit keeps history clean.
- Findings that are purely cosmetic (e.g. comment typos) should be
  batched into a single "style cleanup" commit.
- Never write outside the repository directory.
- Do NOT use repository default branch as diff base when a PR exists.
- Do NOT continue if PR `base.ref` cannot be resolved and no caller base is
  provided.
