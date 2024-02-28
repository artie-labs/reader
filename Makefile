.PHONY: static
static:
	staticcheck ./...

.PHONY: test
test:
	go test ./...

.PHONY: integration-test
itest:
	printenv
	go run sources/postgres/integration_test/main.go

.PHONY: race
race:
	go test -race ./...

.PHONY: build
build:
	goreleaser build --clean

.PHONY: release
release:
	goreleaser release --clean

.PHONY: clean
clean:
	go clean -testcache
