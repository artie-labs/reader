.PHONY: static
static:
	staticcheck ./...

.PHONY: test
test:
	go test ./...

.PHONY: mongo-itest
mongo-itest:
	go run integration_tests/mongo/main.go

.PHONY: mysql-itest
mysql-itest:
	go run integration_tests/mysql/main.go

.PHONY: postgres-itest
postgres-itest:
	go run integration_tests/postgres/main.go

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

.PHONY: generate
generate:
	go get github.com/maxbrunsfeld/counterfeiter/v6
	go generate ./...
	go mod tidy
