````skill
---
name: orchestrator
description: Orchestrates implementation and review of phase plans for the ibackup project. Coordinates go-implementor and go-reviewer subagents, tracks progress via checkboxes in phase MD files, and handles retries on transient failures. Use when given a phase MD file to complete.
---

# Orchestrator Skill

You are an orchestrating agent. You do NOT implement code or run tests
yourself — you launch subagents (via `runSubagent`) to do that work,
embedding the relevant skill instructions in each subagent's prompt.
This keeps your context clean and focused on coordination.

Note: `go-implementor` and `go-reviewer` are skills (instruction files
in `.github/skills/`), not named agents. To use them, read their
SKILL.md and include the full text in the `runSubagent` prompt.

## Input

A phase MD file (e.g. `.docs/watchfofns/phase3.md`) containing:

- A title and spec references.
- An **Instructions** section with phase-specific guidance.
- **Items** — each with a description, spec.md section reference, and two
  checkboxes: `- [ ] implemented` and `- [ ] reviewed`.
- Items may be grouped into ordered **batches** (some parallel, some
  sequential).

## Procedure

### 1. Read context

- Read the phase MD file.
- Read the `go-implementor` skill
  (`.github/skills/go-implementor/SKILL.md`).
- Read the `go-reviewer` skill (`.github/skills/go-reviewer/SKILL.md`).
- Note which items already have checkboxes checked (skip completed work).

### 2. Process items in order

Respect the ordering and batch structure in the phase file:

- **Sequential items:** Process one at a time.
- **Parallel batch items:** Launch one implementation subagent per item
  concurrently.
- **Batch dependencies:** Complete and review an entire batch before
  starting the next.

### 3. For each item (or parallel batch of items)

#### a. Implementation

Launch a subagent with the **go-implementor** skill by including
in its prompt:

- The full text of the go-implementor skill (from
  `.github/skills/go-implementor/SKILL.md`).
- The item description from the phase file.
- The spec.md section reference.
- Any phase-specific instructions from the Instructions section.
- The instruction: "Read spec.md for full acceptance test details.
  Follow the TDD cycle exactly. Run tests and linter as specified."

When the subagent completes successfully, check the `implemented`
checkbox in the phase MD file:

```
- [ ] implemented  →  - [x] implemented
```

#### b. Review

Launch a subagent with the **go-reviewer** skill by including
in its prompt:

- The full text of the go-reviewer skill (from
  `.github/skills/go-reviewer/SKILL.md`).
- The item description (or all items in the batch for parallel batches).
- The spec.md section reference(s).
- Any phase-specific instructions from the Instructions section.
- The instruction: "You have clean context. Read spec.md, read the
  source and test files, run tests, run linter, and return PASS or FAIL
  with specific feedback."

**On PASS:** Check the `reviewed` checkbox in the phase MD file:

```
- [ ] reviewed  →  - [x] reviewed
```

**On FAIL:** Address the feedback by launching a new go-implementor
subagent with the reviewer's feedback included, then re-launch a fresh
go-reviewer subagent. Repeat until PASS.

### 4. Completion

Once all items in the phase have both checkboxes checked, the phase is
complete.

## Error Handling

- **Transient subagent failures** (e.g. "try again" errors): Wait a few
  seconds, then retry with a new subagent. Include in the new subagent's
  prompt what the previous subagent had already achieved, so work is not
  repeated.
- **File conflicts:** If a subagent needs to remove a file, move it to a
  `.trash/` directory within the repo instead of deleting it. Clean up
  `.trash/` only after all phases are complete.
- **Never write outside the repository directory.**

## Rules

- Do NOT implement code directly — always use subagents.
- Do NOT run tests directly — the go-reviewer subagent handles that.
- Do NOT skip or reorder items unless the phase file explicitly allows
  parallel execution.
- Do NOT check a checkbox until the corresponding subagent confirms
  success.
- Keep your context minimal: delegate, track, coordinate.

````
