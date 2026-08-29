package model_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// guardCombos is every combination of the guard inputs the machines can read:
// the six flags, a reason code present or not, a defer reason present or not,
// and available_at before, at and after now. 768 of them, enumerated rather
// than sampled, which is what makes the sweeps below proofs rather than
// evidence. Nothing here is random, so nothing here can be flaky.
func guardCombos() []model.Guards {
	const flags = 7

	var out []model.Guards
	for _, availableAt := range []int64{past, now, future} {
		for _, reason := range []string{"", reasonCode} {
			for _, why := range []string{"", deferBecause} {
				for mask := range 1 << flags {
					out = append(out, model.Guards{
						Now:              now,
						AvailableAt:      availableAt,
						CancelRequested:  mask&1 != 0,
						LeaseValid:       mask&2 != 0,
						AttemptsLeft:     mask&4 != 0,
						AnyStepFailed:    mask&8 != 0,
						AllStepsTerminal: mask&16 != 0,
						CrashBudgetLeft:  mask&32 != 0,
						AnyStepCancelled: mask&64 != 0,
						ReasonCode:       reason,
						DeferReason:      why,
					})
				}
			}
		}
	}
	return out
}

// pair is one cell of a cross table.
type pair struct {
	state string
	event string
}

// machine is one of the two state machines seen through the shape they share,
// so every sweep below runs over both.
type machine struct {
	kind      string
	initial   model.State
	states    []model.State
	next      func(model.State, model.Event, model.Guards) (model.State, []model.Effect, error)
	legal     map[pair]bool
	wantPairs int
	wantLegal int
}

func machines() []machine {
	return []machine{runMachine(), stepMachine()}
}

func runMachine() machine {
	states := make([]model.State, 0, len(model.AllRunStates()))
	for _, s := range model.AllRunStates() {
		states = append(states, s)
	}
	legal := map[pair]bool{}
	for _, tc := range runTable() {
		if tc.err == nil {
			legal[pair{tc.from.String(), tc.event.String()}] = true
		}
	}
	return machine{
		kind:    "run",
		initial: model.RunQueued,
		states:  states,
		next: func(s model.State, ev model.Event, g model.Guards) (model.State, []model.Effect, error) {
			return model.NextRunState(s.(model.RunState), ev, g)
		},
		legal:     legal,
		wantPairs: 55,
		wantLegal: 9,
	}
}

func stepMachine() machine {
	states := make([]model.State, 0, len(model.AllStepStates()))
	for _, s := range model.AllStepStates() {
		states = append(states, s)
	}
	legal := map[pair]bool{}
	for _, tc := range stepTable() {
		if tc.err == nil {
			legal[pair{tc.from.String(), tc.event.String()}] = true
		}
	}
	return machine{
		kind:    "step",
		initial: model.StepPending,
		states:  states,
		next: func(s model.State, ev model.Event, g model.Guards) (model.State, []model.Effect, error) {
			return model.NextStepState(s.(model.StepState), ev, g)
		},
		legal:     legal,
		wantPairs: 66,
		wantLegal: 9,
	}
}

// TestCrossTableIsComplete is the acceptance criterion that every cell of
// state times event is decided. A pair the table calls legal must never come
// back as an illegal transition; a pair the table leaves out must come back as
// one under every guard combination there is, without moving the machine and
// without demanding an effect.
//
// The two directions pin the table and the code to each other. Deleting a legal
// transition from the code fails the row in the table that names it; deleting
// the row instead fails here, because the machine then allows a pair the table
// says is illegal.
func TestCrossTableIsComplete(t *testing.T) {
	combos := guardCombos()

	for _, m := range machines() {
		t.Run(m.kind, func(t *testing.T) {
			pairs, legal := 0, 0
			for _, from := range m.states {
				for _, ev := range model.AllEvents() {
					pairs++
					if m.legal[pair{from.String(), ev.String()}] {
						legal++
						checkLegalPair(t, m, from, ev, combos)
						continue
					}
					checkIllegalPair(t, m, from, ev, combos)
				}
			}
			if pairs != m.wantPairs {
				t.Errorf("swept %d pairs of the %s cross table, want %d: a state or an event has gone missing",
					pairs, m.kind, m.wantPairs)
			}
			if legal != m.wantLegal {
				t.Errorf("the %s table names %d legal pairs, want %d: adding or removing one is a rule change",
					m.kind, legal, m.wantLegal)
			}
		})
	}
}

// checkLegalPair proves a pair the table calls legal is never reported as an
// illegal transition, and that at least one guard combination gets through.
// A guard refusal is fine: the pair exists, the guards said not now.
func checkLegalPair(t *testing.T, m machine, from model.State, ev model.Event, combos []model.Guards) {
	t.Helper()

	allowed := 0
	for _, g := range combos {
		_, _, err := m.next(from, ev, g)
		if errors.Is(err, model.ErrIllegalTransition) {
			t.Fatalf("%s %q + %q is in the table but the machine calls it illegal under %+v",
				m.kind, from, ev, g)
		}
		if err == nil {
			allowed++
		}
	}
	if allowed == 0 {
		t.Errorf("%s %q + %q is in the table but no guard combination gets through it",
			m.kind, from, ev)
	}
}

// checkIllegalPair proves a pair the table leaves out is refused under every
// guard combination, that the refusal names both sides, and that it changes
// nothing.
func checkIllegalPair(t *testing.T, m machine, from model.State, ev model.Event, combos []model.Guards) {
	t.Helper()

	for _, g := range combos {
		got, fx, err := m.next(from, ev, g)
		if !errors.Is(err, model.ErrIllegalTransition) {
			t.Fatalf("%s %q + %q is not in the table but the machine gave (%q, %v, %v) under %+v",
				m.kind, from, ev, got, fx, err, g)
		}
		var detail model.IllegalTransitionError
		if !errors.As(err, &detail) {
			t.Fatalf("%s %q + %q was refused with %v, which carries no from state and event",
				m.kind, from, ev, err)
		}
		if detail.From.String() != from.String() || detail.Event != ev {
			t.Fatalf("%s %q + %q was refused naming (%q, %q)",
				m.kind, from, ev, detail.From, detail.Event)
		}
		if got.String() != from.String() || len(fx) != 0 {
			t.Fatalf("%s %q + %q was refused but gave (%q, %v): a refusal changes nothing",
				m.kind, from, ev, got, fx)
		}
	}
}

// TestOnlyOperatorRetryLeavesATerminalRun is 02 T14 as a sweep: a finished run
// moves for one event and no other, whatever the guards say.
func TestOnlyOperatorRetryLeavesATerminalRun(t *testing.T) {
	combos := guardCombos()

	for _, from := range model.AllRunStates() {
		if !from.IsTerminal() {
			continue
		}
		for _, ev := range model.AllEvents() {
			for _, g := range combos {
				got, fx, err := model.NextRunState(from, ev, g)
				if ev == model.EvOperatorRetry {
					if err != nil || got != model.RunQueued {
						t.Fatalf("run %q + %q gave (%q, %v), want a reopened run", from, ev, got, err)
					}
					continue
				}
				if !errors.Is(err, model.ErrIllegalTransition) || got != from || len(fx) != 0 {
					t.Fatalf("run %q accepted %q, giving (%q, %v, %v) under %+v: only an operator retry leaves a terminal state",
						from, ev, got, fx, err, g)
				}
			}
		}
	}
}

// TestATerminalStepAcceptsNoEvent is the same rule one level down, with the
// one exception M4-04 writes into the machine: an operator reopening the run
// puts its failed and skipped steps back in front of the claim gate. A
// succeeded or cancelled step still accepts nothing; their outcomes are what
// a retry builds on.
func TestATerminalStepAcceptsNoEvent(t *testing.T) {
	combos := guardCombos()

	for _, from := range model.AllStepStates() {
		if !from.IsTerminal() {
			continue
		}
		for _, ev := range model.AllEvents() {
			reopenable := (from == model.StepFailed || from == model.StepSkipped) &&
				ev == model.EvOperatorRetry
			for _, g := range combos {
				got, fx, err := model.NextStepState(from, ev, g)
				if reopenable {
					if err != nil || got != model.StepPending {
						t.Fatalf("step %q + %q gave (%q, %v), want a reopened step",
							from, ev, got, err)
					}
					continue
				}
				if !errors.Is(err, model.ErrIllegalTransition) || got != from || len(fx) != 0 {
					t.Fatalf("step %q accepted %q, giving (%q, %v, %v) under %+v: only an operator retry reopens a failed or skipped step",
						from, ev, got, fx, err, g)
				}
			}
		}
	}
}

// TestEveryStateIsReachableAndNothingEscapes walks both machines from their
// starting state, following only transitions the guards allow, until no new
// state turns up. Two properties come out of the walk: no sequence of legal
// events ever produces a state outside the closed set, and every state in that
// set is reachable, so the set holds no state the machines cannot enter.
func TestEveryStateIsReachableAndNothingEscapes(t *testing.T) {
	combos := guardCombos()

	for _, m := range machines() {
		t.Run(m.kind, func(t *testing.T) {
			known := map[string]bool{}
			for _, s := range m.states {
				known[s.String()] = true
			}

			seen := map[string]bool{m.initial.String(): true}
			queue := []model.State{m.initial}
			for len(queue) > 0 {
				from := queue[0]
				queue = queue[1:]
				for _, ev := range model.AllEvents() {
					for _, g := range combos {
						got, _, err := m.next(from, ev, g)
						if err != nil {
							continue
						}
						if !known[got.String()] {
							t.Fatalf("%s %q + %q escaped the closed set with %q",
								m.kind, from, ev, got)
						}
						if !seen[got.String()] {
							seen[got.String()] = true
							queue = append(queue, got)
						}
					}
				}
			}

			for _, s := range m.states {
				if !seen[s.String()] {
					t.Errorf("%s state %q is not reachable from %q by any legal sequence",
						m.kind, s, m.initial)
				}
			}
		})
	}
}

// TestEveryTerminalOutcomeNeedsAReasonCode is the 06 section 2.1 gate, derived
// from the tables rather than listed again: take every legal transition that
// ends in a terminal state, remove the reason code, and the machine must refuse
// it. Nothing reaches an end without an explanation.
func TestEveryTerminalOutcomeNeedsAReasonCode(t *testing.T) {
	checked := 0
	for _, tc := range runTable() {
		if tc.err != nil || !tc.want.IsTerminal() {
			continue
		}
		checked++
		g := tc.guards
		g.ReasonCode = ""
		got, fx, err := model.NextRunState(tc.from, tc.event, g)
		if !errors.Is(err, model.ErrMissingReasonCode) || got != tc.from || len(fx) != 0 {
			t.Errorf("run %q + %q reached %q without a reason code: (%q, %v, %v)",
				tc.from, tc.event, tc.want, got, fx, err)
		}
	}
	for _, tc := range stepTable() {
		if tc.err != nil || !tc.want.IsTerminal() {
			continue
		}
		checked++
		g := tc.guards
		g.ReasonCode = ""
		got, fx, err := model.NextStepState(tc.from, tc.event, g)
		if !errors.Is(err, model.ErrMissingReasonCode) || got != tc.from || len(fx) != 0 {
			t.Errorf("step %q + %q reached %q without a reason code: (%q, %v, %v)",
				tc.from, tc.event, tc.want, got, fx, err)
		}
	}
	// Three run outcomes and four step outcomes are terminal. Below that the
	// tables have lost a row and this check would pass on nothing.
	if checked < 7 {
		t.Errorf("checked %d terminal outcomes, want at least 7: the tables have shrunk under the gate", checked)
	}
}

// TestEveryGuardIsLoadBearing sweeps both cross tables and flips one guard
// field at a time. A field no outcome depends on is decoration: it suggests an
// enforcement the model does not make, which is worse than not having the field
// at all.
func TestEveryGuardIsLoadBearing(t *testing.T) {
	flips := []struct {
		name string
		flip func(model.Guards) model.Guards
	}{
		{"CancelRequested", func(g model.Guards) model.Guards { g.CancelRequested = !g.CancelRequested; return g }},
		{"LeaseValid", func(g model.Guards) model.Guards { g.LeaseValid = !g.LeaseValid; return g }},
		{"AttemptsLeft", func(g model.Guards) model.Guards { g.AttemptsLeft = !g.AttemptsLeft; return g }},
		{"AnyStepFailed", func(g model.Guards) model.Guards { g.AnyStepFailed = !g.AnyStepFailed; return g }},
		{"AllStepsTerminal", func(g model.Guards) model.Guards { g.AllStepsTerminal = !g.AllStepsTerminal; return g }},
		{"CrashBudgetLeft", func(g model.Guards) model.Guards { g.CrashBudgetLeft = !g.CrashBudgetLeft; return g }},
		{"ReasonCode", func(g model.Guards) model.Guards {
			if g.ReasonCode == "" {
				g.ReasonCode = reasonCode
			} else {
				g.ReasonCode = ""
			}
			return g
		}},
		{"DeferReason", func(g model.Guards) model.Guards {
			if g.DeferReason == "" {
				g.DeferReason = deferBecause
			} else {
				g.DeferReason = ""
			}
			return g
		}},
		{"AvailableAt", func(g model.Guards) model.Guards {
			if g.AvailableAt > g.Now {
				g.AvailableAt = past
			} else {
				g.AvailableAt = future
			}
			return g
		}},
	}

	combos := guardCombos()
	for _, f := range flips {
		t.Run(f.name, func(t *testing.T) {
			for _, m := range machines() {
				for _, from := range m.states {
					for _, ev := range model.AllEvents() {
						for _, g := range combos {
							if differs(m, from, ev, g, f.flip(g)) {
								return
							}
						}
					}
				}
			}
			t.Errorf("no outcome anywhere depends on %s: the field promises an enforcement the model does not make", f.name)
		})
	}
}

// differs reports whether two guard sets lead to different outcomes for one
// pair. Errors are compared by their message, which is the whole outcome a
// caller sees.
func differs(m machine, from model.State, ev model.Event, a, b model.Guards) bool {
	stateA, fxA, errA := m.next(from, ev, a)
	stateB, fxB, errB := m.next(from, ev, b)
	if stateA.String() != stateB.String() || !slices.Equal(fxA, fxB) {
		return true
	}
	return errText(errA) != errText(errB)
}
