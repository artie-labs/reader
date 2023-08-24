.PHONY: test
test:
	go test ./...

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
