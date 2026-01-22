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

test:
	@go test -tags netgo -timeout 40m --count 1 -v .
	@go test -tags netgo --count 1 $(shell go list ./... | grep -v '^${PKG}$$')

race: race-subpkgs
	@$(MAKE) race-main

race-main:
	@go test -tags netgo -timeout 40m -race --count 1 -v .

race-subpkgs:
	@go test -tags netgo -race --count 1 $(shell go list ./... | grep -v '^${PKG}$$')

bench:
	go test -tags netgo --count 1 -run Bench -bench=. ./...

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

.PHONY: test race race-main race-subpkgs bench lint build install clean dist
