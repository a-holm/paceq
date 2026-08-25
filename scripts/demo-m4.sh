#!/bin/sh
# The M4 exit demo for paceq, issue #20: the milestone's exit criterion as a
# runnable script. A diamond DAG runs green with its two branches admitted
# as true siblings, one branch then fails on purpose, everything downstream
# of the failure ends skipped with its own reason code, and one operator
# retry reopens exactly the failed and skipped steps while the succeeded
# ones are reused untouched. The retried run goes green in place.
#
# Two IPC modes, both proven here:
#
#   ./scripts/demo-m4.sh --down   no socket ever exists; every operator
#                                 write takes the flock path straight into
#                                 the database (default)
#   ./scripts/demo-m4.sh --up     the daemon serves .paceq/paceq.sock; the
#                                 same retry travels the socket instead
#
# The same story lives as CI-enforced testscript rows:
# internal/cli/testdata/dagdemo/dag_demo_down.txtar and dag_demo_up.txtar.
# This script is their human-readable twin.
#
# The parallel half of the criterion - the two branches' started/finished
# windows actually overlapping in time - is asserted by the overlap row,
# internal/cli/testdata/dagdemo/dag_demo_overlap.txtar, which drives the
# production claim gate and worker pool directly; today's foreground engine
# executes one step at a time, so a CLI run cannot show that overlap yet.

set -eu

mode=down
keep=0
for arg in "$@"; do
	case $arg in
	--down) mode=down ;;
	--up) mode=up ;;
	--keep) keep=1 ;;
	*)
		echo "usage: $0 [--down|--up] [--keep]" >&2
		exit 64
		;;
	esac
done

command -v paceq >/dev/null 2>&1 || {
	echo "paceq is not on PATH; build it first: go build -o <somewhere-on-PATH> ./cmd/paceq" >&2
	exit 69
}
command -v python3 >/dev/null 2>&1 || {
	echo "python3 is needed to read the JSON records" >&2
	exit 69
}

repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
diamond=$repo/examples/dag/diamond.yaml
[ -f "$diamond" ] || {
	echo "the diamond example is missing at $diamond" >&2
	exit 66
}

demo=$(mktemp -d "${TMPDIR:-/tmp}/demo-m4.XXXXXX")
if [ "$keep" = 1 ]; then
	echo "demo project: $demo"
else
	trap 'rm -rf "$demo"' EXIT
fi
cd "$demo"

fail() {
	echo "M4 demo FAILED ($mode mode): $*" >&2
	exit 1
}

# record FIELD FILE prints one field of a {"run":{...}} record; FIELD may
# reach into the steps array as steps.NAME.FIELD.
record() {
	python3 -c 'import json,sys
doc = json.load(open(sys.argv[2]))["run"]
path = sys.argv[1].split(".", 2)
if path[0] == "steps":
    for step in doc["steps"]:
        if step["name"] == path[1]:
            print(step[path[2]])
            break
else:
    print(doc[sys.argv[1]])' "$1" "$2"
}

steps_of() {
	python3 -c 'import json,sys
doc = json.load(open(sys.argv[1]))["run"]
for step in doc["steps"]:
    print(step["name"], step["state"], step.get("reason_code", "-"))' "$1"
}

await_state() {
	rid=$1
	want=$2
	file=$3
	i=0
	state=unreadable
	while [ "$i" -lt 300 ]; do
		if paceq runs show "$rid" -o json >"$file" 2>/dev/null; then
			state=$(record state "$file")
			[ "$state" = "$want" ] && return 0
		fi
		i=$((i + 1))
		sleep 0.2
	done
	fail "run $rid never reached $want (last state $state)"
}

mkdir -p jobs effects marks
cp "$diamond" jobs/diamond.yaml

echo "== M4 demo, $mode IPC mode =="
paceq init >/dev/null
paceq apply >/dev/null

echo "-- phase 1: the diamond runs green"
paceq run diamond >/dev/null || fail "the first diamond run did not succeed"
paceq runs list --limit 1 -o json >green.json
green=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[0]["id"])' green.json)
paceq runs show "$green" -o json >green_show.json
state=$(record state green_show.json)
[ "$state" = succeeded ] || fail "run $green ended $state"
echo "   green run $green"

echo "-- phase 2: the warehouse branch fails, downstream leaves skipped"
: >marks/fail-warehouse
rc=0
paceq run diamond >failed.json 2>failed.err || rc=$?
[ "$rc" = 5 ] || fail "the blocked run exited $rc, want 5 (job failed, paceq worked)"
grep -q '"reason_code":"RUN_FAILED_STEP"' failed.json ||
	fail "the run record does not say RUN_FAILED_STEP"
failed=$(record id failed.json)

steps=$(steps_of failed.json)
echo "$steps" | grep -q '^load-warehouse failed STEP_FAILED_NONZERO_EXIT$' ||
	fail "load-warehouse did not end failed: $steps"
echo "$steps" | grep -q '^publish skipped STEP_SKIPPED_UPSTREAM_FAILED$' ||
	fail "publish is not skipped with UPSTREAM_FAILED: $steps"
echo "$steps" | grep -q '^report skipped STEP_SKIPPED_UPSTREAM_SKIPPED$' ||
	fail "report is not skipped with UPSTREAM_SKIPPED: $steps"
echo "$steps" | grep -q '^notify skipped STEP_SKIPPED_UPSTREAM_SKIPPED$' ||
	fail "notify is not skipped with UPSTREAM_SKIPPED: $steps"
echo "$steps" | grep -q '^load-cache succeeded STEP_SUCCEEDED$' ||
	fail "load-cache did not stay succeeded: $steps"
echo "   failure closed publish (UPSTREAM_FAILED) and report+notify (UPSTREAM_SKIPPED)"

count_effects() {
	rid=$1
	step=$2
	file="effects/$rid.$step.txt"
	if [ -f "$file" ]; then
		wc -l <"$file"
	else
		echo 0
	fi
}

check_effect() {
	rid=$1
	step=$2
	want=$3
	got=$(count_effects "$rid" "$step")
	[ "$got" = "$want" ] || fail "$step wrote $got effects this run, want $want"
}

echo "-- phase 3: the retry reopens exactly the failed and skipped steps"
rm -f marks/fail-warehouse
serve_pid=
if [ "$mode" = up ]; then
	paceq serve --workers 1 --socket .paceq/paceq.sock >serve.log 2>&1 &
	serve_pid=$!
	i=0
	while [ "$i" -lt 150 ]; do
		[ -S .paceq/paceq.sock ] && break
		sleep 0.2
		i=$((i + 1))
	done
	[ -S .paceq/paceq.sock ] || fail "the daemon's socket never appeared"
	echo "   daemon serving .paceq/paceq.sock; the retry below travels it"
fi

paceq runs retry "$failed" -o json >retry.json ||
	fail "runs retry refused the failed run"
reopened=$(python3 -c 'import json,sys
print(json.dumps(json.load(open(sys.argv[1]))["reopened"]))' retry.json)
epoch=$(python3 -c 'import json,sys
print(json.load(open(sys.argv[1]))["new_epoch"])' retry.json)
[ "$reopened" = '["load-warehouse", "publish", "report", "notify"]' ] ||
	fail "the retry reopened $reopened, want exactly the failed and skipped steps"
[ "$epoch" = 2 ] || fail "the retry left the lease epoch at $epoch, want 2"
echo "   retry reopened $reopened at epoch $epoch"

if [ "$mode" = down ]; then
	check_effect "$failed" extract 1
	check_effect "$failed" load-cache 1
	check_effect "$failed" load-warehouse 0
	echo "   nothing has executed yet; the succeeded steps are untouched"
	paceq serve --workers 1 >serve.log 2>&1 &
	serve_pid=$!
	echo "   a bare serve drains the queue; every CLI write stays on flock"
fi

await_state "$failed" succeeded final.json

kill -TERM "$serve_pid"
wait "$serve_pid"

for step in extract transform load-warehouse publish report load-cache notify; do
	check_effect "$failed" "$step" 1
done
attempt2=$(paceq logs "$failed" --step load-warehouse --attempt 2 | wc -l)
[ "$attempt2" -ge 1 ] || fail "load-warehouse attempt 2 left no log lines"
paceq fsck >/dev/null || fail "fsck found violations after the demo"

echo "   retried run $failed succeeded in place; effects prove the reuse"
echo "== M4 demo OK ($mode mode) =="
