ANTLR_DIR = lib/antlr/generated
GENERATED_ANTLR_DIR = github.com/artie-labs/reader/lib/antlr/generated

.PHONY: antlr
antlr:
	cd $(ANTLR_DIR); antlr -package generated -Dlanguage=Go *.g4

.PHONY: vet
vet:
	go vet $(go list ./... | grep -v $(GENERATED_ANTLR_DIR))

.PHONY: static
static:
	staticcheck $(go list ./... | grep -v $(GENERATED_ANTLR_DIR))

.PHONY: test
test:
	go test ./...

.PHONY: mongo-itest
mongo-itest:
	go run integration_tests/mongo/main.go

.PHONY: mssql-itest
mssql-itest:
	go run integration_tests/mssql/main.go

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

.PHONY: upgrade
upgrade:
	go get github.com/artie-labs/transfer
	go mod tidy
	echo "Upgrade complete"
