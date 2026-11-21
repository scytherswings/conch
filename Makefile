SHELL := /bin/sh

.PHONY: help test test-go test-arbiter build fmt vet lint docker validate clean

help:
	@printf '%s\n' \
		'make test          run the Go and arbiter suites' \
		'make test-go       run the Go suite' \
		'make test-arbiter  run the Workers vitest suite' \
		'make build         build ./conch' \
		'make fmt           gofmt the Go tree' \
		'make vet           go vet the Go tree' \
		'make lint          fail on unformatted Go or vet findings' \
		'make docker        build the conch image' \
		'make validate      validate the shipped example configs' \
		'make clean         remove the built binary'

test: test-go test-arbiter

test-go:
	go test ./...

arbiter/node_modules: arbiter/package-lock.json
	cd arbiter && npm ci

test-arbiter: arbiter/node_modules
	cd arbiter && npm test

build:
	go build -o conch ./cmd/conch

fmt:
	gofmt -w .

vet:
	go vet ./...

lint:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		printf 'gofmt needed:\n%s\n' "$$unformatted" >&2; \
		exit 1; \
	fi
	go vet ./...

docker:
	docker build -t conch:dev .

validate:
	go run ./cmd/conch validate --config deploy/config.example.host-a.yml
	go run ./cmd/conch validate --config deploy/config.example.host-b.yml
	go run ./cmd/conch validate --config examples/config.minimal.yml

clean:
	rm -f conch
