# Phase 10: CLI and integration (cmd/ + main_test.go)

Ref: [spec.md](spec.md) sections I1, J1

## Instructions

Use the `orchestrator` skill to complete this phase, coordinating
subagents with the `go-implementor` and `go-reviewer` skills.

- Integration tests must use `client.PretendSubmissions` to avoid needing a real
  wr server, and `internal.LocalHandler` to avoid needing real iRODS, as
  described in spec.md (Appendix > "Testing strategy").

## Items

These items MUST be implemented in the listed order. J1 depends on I1.

### Item 10.1: I1 - ibackup watchfofns subcommand

spec.md section: I1

Implement the cobra subcommand in cmd/watchfofns.go with all flags listed in
spec.md section I1. Write GoConvey tests in main_test.go covering all 3
acceptance tests from spec.md section I1.

- [x] implemented
- [x] reviewed

### Item 10.2: J1 - End-to-end integration test

spec.md section: J1

Depends on I1. Write the full end-to-end integration test in main_test.go
using PretendSubmissions and LocalHandler. Cover all 5 acceptance tests from
spec.md section J1 (new fofn processing, freeze mode, restart resilience,
config.yml creation helper, buried jobs with fofn update).

- [x] implemented
- [x] reviewed

