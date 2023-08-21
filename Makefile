.PHONY: test
test:
	go test ./...

.PHONY: race
race:
	go test -race ./...
