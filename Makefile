PKG := github.com/wtsi-hgi/ibackup
VERSION := $(shell git describe --tags --always --long --dirty)
TAG := $(shell git describe --abbrev=0 --tags)
LDFLAGS = -ldflags "-X ${PKG}/cmd.Version=${VERSION}"
export GOPATH := $(shell go env GOPATH)
PATH := ${PATH}:${GOPATH}/bin
MAKEFLAGS += --no-print-directory

default: install

# We require CGO_ENABLED=1 for getting group information to work properly; the
# pure go version doesn't work on all systems such as those using LDAP for
# groups
export CGO_ENABLED = 1

build:
	go build -tags netgo ${LDFLAGS}

install:
	@rm -f ${GOPATH}/bin/ibackup
	@go install -tags netgo ${LDFLAGS}
	@echo installed to ${GOPATH}/bin/ibackup

# The main package's end-to-end tests spend most of their time waiting on iRODS
# and wr, so its top-level tests run as separate processes of one test binary,
# MAIN_TEST_JOBS at a time, the slowest first. Each test's output is printed
# when it finishes. The timeout is for the whole run: each process gets what
# is left of it. Set MAIN_TESTS to run only some of the tests.
MAIN_TEST_JOBS ?= 4
MAIN_TESTS ?= $(shell sed -n 's/^func \(Test[A-Za-z0-9_]*\)(t \*testing\.T).*/\1/p' *_test.go)
MAIN_TESTS_SLOWEST_FIRST := TestEdit TestTrashRemove TestTrashRemovePaths TestTrashRemoveShared \
	TestRemove TestRemoveFile TestRemoveDirs TestRemoveItems TestPuts

define test-main
	@d=$$(mktemp -d) && trap 'rm -rf "$$d"' EXIT && trap 'exit 130' INT TERM HUP && \
	start=$$(date +%s) && end=$$(( start + $(2) )) && \
	go test -tags netgo $(1) -c -o "$$d/main.test" . && \
	printf '%s\n' $(filter $(MAIN_TESTS),$(MAIN_TESTS_SLOWEST_FIRST)) \
		$(filter-out $(MAIN_TESTS_SLOWEST_FIRST),$(MAIN_TESTS)) | \
	xargs -n 1 -P $(MAIN_TEST_JOBS) sh -c 'left=$$(( $$2 - $$(date +%s) )); [ $$left -gt 0 ] || left=1; \
		"$$1/main.test" -test.run "^$$3$$" -test.count 1 -test.v=true -test.timeout $${left}s > "$$1/$$3.log" 2>&1 || \
		{ grep -q "panic: test timed out after" "$$1/$$3.log" && echo "$$3 (timed out)" || echo "$$3"; } >> "$$1/failed"; \
		flock "$$1/failed.lock" cat "$$1/$$3.log"' sh "$$d" "$$end" && \
	took=$$(( $$(date +%s) - start )) && \
	if [ -s "$$d/failed" ]; then echo "FAIL: $$(sort "$$d/failed" | tr '\n' ' ')"; exit 1; fi && \
	printf 'ok  main package tests passed in %dm%02ds\n' $$(( took / 60 )) $$(( took % 60 ))
endef

test:
	$(call test-main,,7200)
	@go test -tags netgo --count 1 $(shell go list ./... | grep -v '^${PKG}$$')

race: race-subpkgs
	@$(MAKE) race-main

race-main:
	$(call test-main,-race,3600)

race-subpkgs:
	@go test -tags netgo -race --count 1 $(shell go list ./... | grep -v '^${PKG}$$')

bench:
	go test -tags netgo --count 1 -run Bench -bench=. ./...

# Compares the speed of the critical upload path to origin/develop, failing if
# it is over 10% slower. See developers/README.md for the SPEED_* options. The
# gate is built and run directly, not with go run, so that its exit code of 1
# (regression) or 2 (could not run) reaches make. On a signal the shell waits
# for the gate to clean up before removing the build dir.
speed:
	@d=$$(mktemp -d) && trap 'rm -rf "$$d"' EXIT && trap 'exit 130' INT TERM HUP && \
	CGO_ENABLED=1 go build -tags netgo -o "$$d/speedgate" ./developers/speedgate && "$$d/speedgate"

# curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(go env GOPATH)/bin v2.6.0
lint:
	@golangci-lint run

clean:
	@rm -f ./ibackup
	@rm -f ./dist.zip

# go get -u github.com/gobuild/gopack
# go get -u github.com/aktau/github-release
dist:
	gopack pack --os linux --arch amd64 -o linux-dist.zip
	github-release release --tag ${TAG} --pre-release
	github-release upload --tag ${TAG} --name ibackup-linux-x86-64.zip --file linux-dist.zip
	@rm -f ibackup linux-dist.zip

.PHONY: test race race-main race-subpkgs bench speed lint build install clean dist
