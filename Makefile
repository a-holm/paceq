GO ?= go
BIN := bin/paceq

# Tool versions. Every gate runs through `go run`, so the pipeline and a local
# `make ci` execute the same tool binaries. Bump versions here; no workflow
# repeats them.
GOFUMPT     := mvdan.cc/gofumpt@v0.11.0
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.0
GOSEC       := github.com/securego/gosec/v2/cmd/gosec@v2.28.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.7.0

# The shipped binary is cgo free. Every target inherits this.
export CGO_ENABLED = 0

# Builds never edit go.mod or go.sum. Drift is reported by tidy-check instead.
export GOFLAGS = -mod=readonly

# Platforms cross built and asserted cgo free on every pull request.
CROSS_TARGETS := linux/amd64 linux/arm64 darwin/arm64

.PHONY: all build test gate bench fmt fmt-check vet staticcheck lint gosec govulncheck \
	tidy-check cross ci hooks clean

all: build

build:
	$(GO) build -trimpath -o $(BIN) ./cmd/paceq

# The race detector is the one thing that needs cgo. It affects the test run only,
# never the artifact built by the build target.
test:
	CGO_ENABLED=1 $(GO) test -race -count=1 ./...

# The concurrency gate and the throughput floor. It runs without the race
# detector on purpose: the detector multiplies the cost of every transaction, so
# a throughput number measured under it says nothing about the driver. The test
# target above still runs the same concurrency assertions with -race, over a
# shorter window.
gate:
	$(GO) test ./internal/store -run 'TestConcurrentWriters|TestWALRecoveryUnderKill|TestLoadHarness' -count=1 -v

# Detailed numbers behind the gate, including what synchronous=FULL costs.
# Not part of ci: a benchmark on a shared runner measures the runner.
bench:
	$(GO) test ./internal/store -run '^$$' -bench . -benchtime 2s -count=1

fmt:
	$(GO) run $(GOFUMPT) -l -w .

fmt-check:
	@out=$$($(GO) run $(GOFUMPT) -l .); \
	if [ -n "$$out" ]; then \
		echo "gofumpt: these files need formatting:"; \
		echo "$$out"; \
		$(GO) run $(GOFUMPT) -d .; \
		echo "run: make fmt"; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

staticcheck:
	$(GO) run $(STATICCHECK) ./...

lint: vet staticcheck

gosec:
	$(GO) run $(GOSEC) -exclude-generated ./...

govulncheck:
	$(GO) run $(GOVULNCHECK) ./...

# -diff reports what `go mod tidy` would rewrite and exits non-zero, without
# touching the files.
tidy-check:
	$(GO) mod tidy -diff

cross:
	@for target in $(CROSS_TARGETS); do \
		scripts/cross-build.sh "$${target%/*}" "$${target#*/}" || exit 1; \
	done

ci: fmt-check vet staticcheck gosec govulncheck tidy-check test gate build cross

hooks:
	git config core.hooksPath .githooks
	@echo "git hooks enabled from .githooks"

clean:
	rm -rf bin
