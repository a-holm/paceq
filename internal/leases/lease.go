package leases

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Default timing per 11 section 4.2: a fifteen second ttl renewed every five
// seconds tolerates two lost renewals before leadership is even in question,
// and a crash costs at most fifteen seconds of scheduler silence, which for a
// cron orchestrator means delayed work, never lost work.
const (
	DefaultTTL   = 15 * time.Second
	DefaultRenew = 5 * time.Second
)

// Store is the lease surface the loop needs. It is the narrow consumer side of
// the store port: the loop never sees a database handle, only these three
// calls, so a test can script them.
type Store interface {
	AcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (store.LeaseGrant, bool, error)
	ReleaseLease(ctx context.Context, name, holder string) (bool, error)
	AppendLeaseEvent(ctx context.Context, e store.LeaseEvent) error
}

// Options configures one role's loop.
type Options struct {
	// Name is the role: "scheduler", "reaper", or "sensor" from M3 on.
	Name string

	// Holder identifies this instance. Production mints one node id at start.
	Holder string

	// TTL and Renew override the defaults. Renew must be at least a third of
	// TTL; anything tighter turns one slow database into a false handover.
	TTL   time.Duration
	Renew time.Duration

	// Clock drives the ticker and the monotonic budget. Nil means
	// clock.System(), which inside a testing/synctest bubble runs on the
	// bubble's virtual clock.
	Clock clock.Clock

	// Log receives one structured line per transition with the fixed fields
	// lease, epoch and holder. Nil means slog.Default().
	Log *slog.Logger
}

// errScriptExhausted is what a scripted test store answers when the loop asks
// for more renewals than the test scripted. It must never surface in
// production; production stores answer every call.
var errScriptExhausted = errors.New("script exhausted")

// RunAsLeader loops until ctx is done: acquire or renew the named lease every
// renew interval, run body while the grant holds, stop it the moment it does
// not. It returns ctx.Err() after releasing the lease, which is the clean
// shutdown path: the next process can take over immediately instead of waiting
// out the ttl.
//
// Leadership decisions are conservative in exactly two ways. A renewal error
// says nothing about who owns the lease, so the body keeps running until the
// monotonic budget since the last confirmed renewal passes the ttl. An empty
// answer says everything, so leadership ends on the spot and the loss is
// recorded once.
func RunAsLeader(ctx context.Context, st Store, opt Options, body func(ctx context.Context, epoch int64) error) error {
	ttl := opt.TTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	renew := opt.Renew
	if renew <= 0 {
		renew = DefaultRenew
	}
	log := opt.Log
	if log == nil {
		log = slog.Default()
	}
	clk := opt.Clock
	if clk == nil {
		clk = clock.System()
	}

	// Bookkeeping writes survive cancellation: losing leadership because ctx
	// fired must still leave the loss row behind.
	book := context.WithoutCancel(ctx)

	var (
		held       bool
		epoch      int64
		lastSeen   int64 // highest fencing token this loop has observed
		confirmed  = clk.Mark()
		cancelBody context.CancelFunc
		bodyDone   <-chan struct{}
	)

	stopBody := func() {
		if cancelBody == nil {
			return
		}
		cancelBody()
		<-bodyDone
		cancelBody = nil
		bodyDone = nil
	}
	record := func(code reason.Code, cause string) {
		if err := st.AppendLeaseEvent(book, store.LeaseEvent{
			At:     clk.Now(),
			Lease:  opt.Name,
			Holder: opt.Holder,
			Epoch:  epoch,
			Code:   code,
		}); err != nil {
			log.Warn("lease event not recorded", "lease", opt.Name, "epoch", epoch,
				"holder", opt.Holder, "code", string(code), "err", err.Error())
		}
		args := []any{"lease", opt.Name, "epoch", epoch, "holder", opt.Holder}
		if cause != "" {
			args = append(args, "cause", cause)
		}
		log.Info(string(code), args...)
	}
	lose := func(cause string) {
		if !held {
			return
		}
		held = false
		stopBody()
		record(reason.LEASELost, cause)
	}
	become := func(g store.LeaseGrant) {
		code := reason.LEASEAcquired
		if g.Epoch > 1 && g.Epoch > lastSeen {
			// A fencing token above one above anything this loop has seen
			// means history happened without us: a dead holder's row was
			// taken. Epoch one, by contrast, is always a fresh row, whether
			// nobody ever held the lease or the last holder deleted it on its
			// way out. Regaining our own former token after an uncertain gap
			// also reads as a fresh claim: we cannot prove anyone took over,
			// so we do not claim it.
			code = reason.LEASETakenOver
		}
		held = true
		epoch = g.Epoch
		lastSeen = g.Epoch
		confirmed = clk.Mark()
		record(code, "")

		bodyCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		e := g.Epoch
		go func() {
			defer close(done)
			defer cancel()
			if err := body(bodyCtx, e); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("lease body failed", "lease", opt.Name, "epoch", e,
					"holder", opt.Holder, "err", err.Error())
			}
		}()
		cancelBody = cancel
		bodyDone = done
	}

	ticker := clk.NewTicker(renew)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			stopBody()
			// No loss row here: a clean shutdown is not a loss to a rival.
			// The deleted row plus the next holder's ACQUIRED event tell the
			// story without borrowing a code whose meaning is "someone else
			// owns a live lease".
			released, err := st.ReleaseLease(book, opt.Name, opt.Holder)
			switch {
			case err != nil:
				log.Warn("lease release failed on shutdown", "lease", opt.Name,
					"holder", opt.Holder, "err", err.Error())
			case released:
				log.Info("released the lease on shutdown", "lease", opt.Name, "holder", opt.Holder)
			}
			return ctx.Err()
		case <-ticker.C:
		}

		g, ok, err := st.AcquireOrRenew(ctx, opt.Name, opt.Holder, ttl)
		switch {
		case err != nil:
			log.Warn("lease renewal errored", "lease", opt.Name, "epoch", epoch,
				"holder", opt.Holder, "err", err.Error())
			if held && clk.Since(confirmed) >= ttl {
				// The budget ran out while the database would not answer.
				// Whether or not anyone took over, we can no longer prove we
				// lead, so stop deciding.
				lose("ttl passed without a confirmed renewal")
			}
		case !ok:
			lose("another holder owns a live lease")
		default:
			if held && g.Epoch == epoch {
				// The quiet path: a steady follower tick spends exactly this
				// one small write and nothing else.
				lastSeen = g.Epoch
				confirmed = clk.Mark()
				break
			}
			if held {
				// Unreachable through the admission statement: a holder that
				// keeps renewing never watches its own epoch move. Treat it
				// as a lost stint anyway rather than keep deciding on a
				// number that stopped making sense.
				log.Warn("the fencing token moved under a live renewal", "lease",
					opt.Name, "epoch", epoch, "seen", g.Epoch, "holder", opt.Holder)
				lose("fencing token moved")
			}
			become(g)
		}
	}
}
