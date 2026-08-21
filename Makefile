GO ?= go
BIN := bin/pulseq

# The shipped binary is cgo free. Every target inherits this.
export CGO_ENABLED = 0

.PHONY: all build test fmt fmt-check lint ci hooks clean

all: build

build:
	$(GO) build -trimpath -o $(BIN) ./cmd/pulseq

# The race detector is the one thing that needs cgo. It affects the test run only,
# never the artifact built by the build target.
test:
	CGO_ENABLED=1 $(GO) test -race ./...

fmt:
	gofumpt -l -w .

fmt-check:
	@out=$$(gofumpt -l .); \
	if [ -n "$$out" ]; then \
		echo "gofumpt: these files need formatting:"; \
		echo "$$out"; \
		echo "run: make fmt"; \
		exit 1; \
	fi

lint:
	$(GO) vet ./...
	staticcheck ./...

ci: fmt-check lint test build

hooks:
	git config core.hooksPath .githooks
	@echo "git hooks enabled from .githooks"

clean:
	rm -rf bin
