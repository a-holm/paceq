GO ?= go
BIN := bin/paceq

# Build metadata, stamped into the binary with -ldflags. The time comes from the
# last commit rather than from the clock, so two builds of one commit produce
# the same binary. Each value falls back to what a plain `go build` reports.
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILDTIME ?= $(shell git log -1 --format=%cI 2>/dev/null || echo unknown)

BUILDVARS := github.com/a-holm/paceq/internal/cli
LDFLAGS   := -X $(BUILDVARS).version=$(VERSION) \
	-X $(BUILDVARS).commit=$(COMMIT) \
	-X $(BUILDVARS).buildTime=$(BUILDTIME)

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

# VCS stamping reads repository metadata. Inside a worktree of a bare checkout
# the metadata is unreachable and every build fails with "error obtaining VCS
# status", so builds run with stamping off everywhere. The Makefile stamps the
# same three values itself with -ldflags, so nothing is lost on the shipped
# binary; test fixtures gain a stable build.
export GOFLAGS += -buildvcs=false

# Platforms cross built and asserted cgo free on every pull request.
CROSS_TARGETS := linux/amd64 linux/arm64 darwin/arm64

.PHONY: all build test property gate chaos bench fuzz fmt fmt-check vet staticcheck lint gosec govulncheck \
	tidy-check cross test-scratch sensors-examples ci fixture-change hooks clean

all: build

build:
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/paceq

# The race detector is the one thing that needs cgo. It affects the test run only,
# never the artifact built by the build target.
test:
	CGO_ENABLED=1 $(GO) test -race -count=1 -timeout 6h ./...

# The concurrency gate and the throughput floor. It runs without the race
# detector on purpose: the detector multiplies the cost of every transaction, so
# a throughput number measured under it says nothing about the driver. The test
# target above still runs the same concurrency assertions with -race, over a
# shorter window.
#
# The parsing budget is here for the same reason. `paceq apply` and shell
# completion both walk a whole jobs directory, and the budget is measured on the
# parser rather than on the detector.
gate:
	$(GO) test ./internal/store -run 'TestConcurrentWriters|TestWALRecoveryUnderKill|TestLoadHarness' -count=1 -v
	$(GO) test ./internal/spec -run 'TestParsingAHundredFilesStaysUnderTheBudget' -count=1 -v

# The seeded SIGKILL chaos sweep (issue #20, AC-9): five hundred runs against a
# real `paceq serve` subprocess that is killed and restarted on a schedule that
# is a pure function of its seed, then the full invariant battery over the
# wreckage. Behind the chaos build tag and therefore absent from `make test`
# and every pull request; the nightly workflow (nightly.yml) runs this target,
# and the ordinary suite carries the small-N smoke of the same machinery. The
# race detector needs cgo, as in the test target; it watches the harness, while
# the daemon under kill is an ordinary child binary. PACEQ_CHAOS_SEED replays a
# named schedule, PACEQ_CHAOS_ARTIFACTS moves the failure archive.
chaos:
	CGO_ENABLED=1 $(GO) test -race -tags chaos -count=1 -timeout 40m -v ./test/chaos

# The sensor-cursor property test (issue #16) is a model-based crash sweep over a
# real SQLite file. It runs here, under the race detector, with a bounded seed and
# action count so it finishes deterministically in CI time. It is deliberately a
# separate step rather than part of the `test` suite above: the race detector
# multiplies the cost of every one of its transactions (a full 100-seed sweep is
# many minutes under -race), so folding it in would drag every PR. Every PR still
# gets it, just through this step; raise -prop.seeds for a deeper sweep.
property:
	CGO_ENABLED=1 $(GO) test -race -tags rapid ./internal/store -run TestSensorCursorProperties -count=1 -prop.seeds=10 -prop.actions=10

# The parser gate. A job file is untrusted input, so the fuzz targets run on
# every pull request rather than nightly only. -count=1 is required with -fuzz
# and is written out anyway, because the repository rule is that no go test in
# the gate may take a cached pass for a real one. The cronx target gives the
# schedule parser the same treatment: expressions are untrusted input too.
FUZZTIME ?= 60s

fuzz:
	$(GO) test ./internal/spec -run '^$$' -fuzz 'FuzzParseJobSpec' -fuzztime $(FUZZTIME) -count=1
	$(GO) test ./internal/cronx -run '^$$' -fuzz 'FuzzCronParse' -fuzztime $(FUZZTIME) -count=1

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
	$(GO) run $(GOSEC) -exclude-generated -exclude-dir=testdata ./...

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

# Scratch container proof for the tzdata embed (issue #47): inside FROM
# scratch there is no /usr/share/zoneinfo, no shell and no network, so the
# binary must find Europe/Oslo through its embedded tzdata and compute the
# next tick for a documented schedule. Skips loudly where docker is missing
# so a local `make ci` stays useful on dockerless machines; CI always has it.
test-scratch:
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "SKIP test-scratch: docker not available"; \
		exit 0; \
	fi
	@mkdir -p bin/scratch
	$(GO) build -trimpath -o bin/scratch/paceq-scratch ./test/scratch
	docker build -q -t paceq-scratch-proof -f test/scratch/Dockerfile .
	@out=$$(docker run --rm --network none paceq-scratch-proof); \
	if [ "$$out" != "2026-01-01T01:00:00Z" ]; then \
		echo "test-scratch: next tick = $$out, want 2026-01-01T01:00:00Z"; \
		exit 1; \
	fi; \
	echo "test-scratch: Europe/Oslo resolves inside FROM scratch, next tick $$out"

# The example sensors (issue #14, M3-07) are shell, not Go, so they get their
# own gate: every script must parse under `sh -n`, and `shellcheck` runs when
# it is installed. The behaviour half lives in Go tests (examples/sensors and
# internal/store), which `make test` already runs. shellcheck is absent on
# some developer machines, so it skips loudly there, exactly like
# test-scratch skips without docker; CI installs it and always runs it.
SENSOR_SCRIPTS := $(wildcard examples/sensors/*.sh examples/sensors/bin/*)
sensors-examples:
	@for f in $(SENSOR_SCRIPTS); do \
		sh -n "$$f" || { echo "sh -n failed for $$f"; exit 1; }; \
	done
	@echo "sensors-examples: $(words $(SENSOR_SCRIPTS)) scripts parse under sh -n"
	@if ! command -v shellcheck >/dev/null 2>&1; then \
		echo "SKIP sensors-examples/shellcheck: shellcheck not installed"; \
	else \
		shellcheck $(SENSOR_SCRIPTS); \
	fi
	@$(GO) test ./examples/sensors/ -count=1
	@$(GO) test ./internal/store/ -run 'TestExampleSensorProductionPath' -count=1

# Gold standard fixtures are expectations, not code: a commit that edits one
# must carry a FIXTURE-CHANGE: line with the reason (plan 04 section 8 point
# 2). The same check runs as a GitHub Actions job on every pull request. It is
# deliberately not part of `make ci`, whose checkout is shallow and has no
# history to walk.
fixture-change:
	scripts/check-fixture-change.sh origin/main HEAD

ci: fmt-check vet staticcheck gosec govulncheck tidy-check test property gate fuzz build test-scratch sensors-examples cross

hooks:
	git config core.hooksPath .githooks
	@echo "git hooks enabled from .githooks"

clean:
	rm -rf bin
