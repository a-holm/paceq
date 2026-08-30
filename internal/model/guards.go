package model

// Guards is everything a machine is allowed to know beyond the current state
// and the event. It is a plain value: the machine reads it, never the database,
// the filesystem or the clock. Time arrives as Now, because internal/clock owns
// time.Now and the model must stay deterministic (M0-09).
//
// Every field is read by at least one transition. A field nothing reads would
// suggest an enforcement that does not exist, so the cross table's outcome
// columns are the proof that each one carries weight.
type Guards struct {
	// Now and AvailableAt are unix milliseconds UTC, the unit of every time
	// column in the database. A run is available when AvailableAt is at or
	// before Now, the same comparison the claim gate in SQL makes.
	Now         int64
	AvailableAt int64
	// CancelRequested is cancel_requested_at being set on the run.
	CancelRequested bool
	// LeaseValid is the run row naming this caller as the lease owner at this
	// caller's fencing epoch. Owner and epoch are the whole rule: the lease
	// deadline is not a term, because it says when the reaper may take the
	// run and not who owns it. A caller whose lease has gone is a stale
	// writer and may not finish or cancel the run; the reaper's
	// EvLeaseExpired is the way that run moves.
	LeaseValid bool
	// AttemptsLeft is the step having retries left under its policy. The
	// backoff itself is M1-09; the model only says the retry transition is
	// allowed.
	AttemptsLeft bool
	// AnyStepFailed, AnyStepCancelled and AllStepsTerminal describe the
	// run's steps. Together they are how the run machine enforces I2 and I10
	// without reading a single step row itself. AnyStepCancelled separates a
	// run whose steps were cancelled from one whose steps all succeeded: both
	// end without a failure, and only the steps say which verdict is honest.
	AnyStepFailed    bool
	AnyStepCancelled bool
	AllStepsTerminal bool
	// CrashBudgetLeft is the poison quarantine (02 section 5.7): a run that
	// has crashed too often is failed instead of requeued.
	CrashBudgetLeft bool
	// ReasonCode is the explanation for a transition that needs one. Every
	// terminal state does (06 section 2.1), and so does the retry transition,
	// which records why the attempt failed. The catalogue of codes is M1-05;
	// the model only insists that there is one.
	ReasonCode string
	// DeferReason is why a run was pushed forward in time. A deferred run
	// always has one (I14), which is what makes deferral explainable without
	// a state of its own.
	DeferReason string
}

// available reports whether a queued run may start now.
func (g Guards) available() bool { return g.AvailableAt <= g.Now }
