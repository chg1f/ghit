all: lint test

.PHONY: lint
lint:
	go fmt ./...
	go vet -v ./...

.PHONY: test
test:
	go clean -testcache && go test -v ./...

.PHONY: gendoc
gendoc:
	gomarkdoc --output README.md --footer "![test workflow](https://github.com/chg1f/ghit/actions/workflows/test.yml/badge.svg?branch=master)" ./...
