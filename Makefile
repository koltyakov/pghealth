# Simple Makefile for pghealth

GO ?= go
PKG := ./
BIN_DIR := bin
BIN := $(BIN_DIR)/pghealth

# Cross-compile settings
PLATFORMS ?= darwin/amd64 darwin/arm64 linux/amd64 linux/arm64 windows/amd64 windows/arm64
DIST := dist
BINARY := pghealth
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS ?= -ldflags "-s -w -X main.version=$(VERSION)"

.PHONY: help deps build build-all test vet fmt check run report clean

help:
	@echo "Targets:"
	@echo "  deps     - tidy modules"
	@echo "  build    - build $(BIN)"
	@echo "  test     - run unit tests"
	@echo "  vet      - run go vet"
	@echo "  fmt      - format code"
	@echo "  check    - fmt + vet + test"
	@echo "  run      - run pghealth (use URL=... OUT=... TIMEOUT=...)"
	@echo "  report   - alias to run"
	@echo "  clean    - remove build artifacts"

deps:
	$(GO) mod tidy

$(BIN):
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o $(BIN) $(PKG)

build: $(BIN)

# Example: make build-all PLATFORMS="linux/amd64 windows/amd64"
build-all:
		@mkdir -p $(DIST)
		@set -e; \
		for p in $(PLATFORMS); do \
			os=$$(echo $$p | cut -d/ -f1); arch=$$(echo $$p | cut -d/ -f2); \
			outdir=$(DIST)/$${os}_$${arch}; \
			mkdir -p $$outdir; \
			out=$$outdir/$(BINARY); \
			if [ "$$os" = "windows" ]; then out="$$out.exe"; fi; \
			echo "Building $$out"; \
			CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build $(LDFLAGS) -o $$out $(PKG); \
		done

test:
	$(GO) test ./...

vet:
	-$(GO) vet ./...

fmt:
	$(GO) fmt ./...

check: fmt vet test

# Usage:
# make run URL=postgres://user:pass@host:5432/db?sslmode=prefer OUT=pghealth_report.html TIMEOUT=30s
run: $(BIN)
	@if [ -n "$(URL)" ]; then \
		$(BIN) --url "$(URL)" --out "$${OUT:-pghealth_report.html}" --timeout "$${TIMEOUT:-30s}"; \
	else \
		$(BIN) --out "$${OUT:-pghealth_report.html}" --timeout "$${TIMEOUT:-30s}"; \
	fi

report: run

clean:
	rm -rf $(BIN_DIR)
	rm -rf $(DIST)
