GO_FILES = $(shell find . -type f -name '*.go')

etcdb: $(GO_FILES)
	go build -o etcdb

etcdb-linux: $(GO_FILES)
	GOOS=linux go build -o etcdb-linux

test:
	go test -v ./... -race

test-integration: etcdb-linux
	basht integration-tests/*.bash

test-deps:
	go get github.com/progrium/basht
