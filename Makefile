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

# The main package's tests can't use t.Parallel (they share process-wide env and
# log output), and mostly wait on iRODS and wr, so its top-level tests run as
# separate processes of one test binary, MAIN_TEST_JOBS at a time. Each test's
# output is printed whole when it finishes, and MAIN_TEST_TIMEOUT bounds each
# test. To run only some tests, use go test -run directly.
MAIN_TEST_JOBS ?= 4
MAIN_TEST_TIMEOUT ?= 30m

define test-main
	@d=$$(mktemp -d) && trap 'rm -rf "$$d"' EXIT && trap 'exit 130' INT TERM HUP && \
	go test -tags netgo $(1) -c -o "$$d/main.test" . && \
	sed -n 's/^func \(Test[A-Za-z0-9_]*\)(t \*testing\.T).*/\1/p' main_test.go | \
	xargs -n 1 -P $(MAIN_TEST_JOBS) sh -c '"$$0/main.test" -test.run "^$$1$$" -test.count 1 -test.v=true \
		-test.timeout $(MAIN_TEST_TIMEOUT) > "$$0/$$1.log" 2>&1; rc=$$?; flock "$$0" cat "$$0/$$1.log"; exit $$rc' "$$d"
endef

# The server package takes close to Go's 10m default test timeout under -race,
# and longer on a busy host, so the sub-package tests get their own bound.
SUBPKG_TEST_TIMEOUT ?= 30m

test:
	$(call test-main)
	@go test -tags netgo --count 1 -timeout $(SUBPKG_TEST_TIMEOUT) $(shell go list ./... | grep -v '^${PKG}$$')

race: race-subpkgs
	@$(MAKE) race-main

race-main:
	$(call test-main,-race)

race-subpkgs:
	@go test -tags netgo -race --count 1 -timeout $(SUBPKG_TEST_TIMEOUT) $(shell go list ./... | grep -v '^${PKG}$$')

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
