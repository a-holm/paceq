package notify

import "sync"

// Topic names one kind of fact worth waking a loop for. A wake carries no
// payload on purpose: the database is the only place facts live, and the wake
// only says "look again now". That is what makes losing one cheap, which in
// turn is what makes the tickers able to carry correctness alone (11 section
// 3.4): the bus is an optimisation, never a dependency.
type Topic string

const (
	// TopicRunQueued means a run entered the queue and the dispatcher should look.
	TopicRunQueued Topic = "run_queued"
	// TopicStepReady means a step became runnable and its executor should look.
	TopicStepReady Topic = "step_ready"
	// TopicScheduleChanged means schedules were applied, paused or resumed.
	TopicScheduleChanged Topic = "schedule_changed"
	// TopicCancelRequested means somebody asked for a cancellation.
	TopicCancelRequested Topic = "cancel_requested"
)

// Bus wakes loops without polling latency. Notify never blocks and never
// queues more than one wake per subscriber: a slow consumer coalesces its
// wakes instead of backing them up, because every wake means the same thing.
//
// A disabled Bus is the --no-notify-bus path. It hands out channels nobody
// ever closes and drops every notify, so a loop runs on its ticker alone with
// no second code path anywhere.
type Bus struct {
	// off silences the bus. Reads take mu; the flag exists so Disabled can
	// share every code path with a live bus.
	off  bool
	mu   sync.Mutex
	subs map[Topic][]chan struct{}
}

// New returns a live bus.
func New() *Bus {
	return &Bus{subs: make(map[Topic][]chan struct{})}
}

// Disabled returns the bus behind --no-notify-bus. Subscribing still works,
// so a loop is written once either way; nothing ever wakes it.
func Disabled() *Bus {
	return &Bus{off: true, subs: make(map[Topic][]chan struct{})}
}

// Subscribe registers one subscriber for a topic and returns its wake channel.
// The channel buffers exactly one wake: that buffer is the whole queue.
func (b *Bus) Subscribe(t Topic) <-chan struct{} {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[t] = append(b.subs[t], ch)
	return ch
}

// Unsubscribe removes a subscriber. The channel it returned goes quiet at
// once, so a loop that stops reading cannot leave a full buffer behind as a
// leak.
func (b *Bus) Unsubscribe(t Topic, sub <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.subs[t][:0]
	for _, ch := range b.subs[t] {
		if ch != sub {
			kept = append(kept, ch)
		}
	}
	b.subs[t] = kept
}

// Notify wakes every subscriber of t. One pending wake per subscriber is the
// cap: a subscriber that has not drained yet is already going to look again,
// and a second wake would say nothing new. The call never blocks and takes
// locks no longer than the fan-out itself.
func (b *Bus) Notify(t Topic) {
	if b.off {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs[t] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Disabled reports whether this bus drops every notify. Loops use it to skip
// subscribing at all, which keeps the --no-notify-bus path free of channels
// nobody will ever send on.
func (b *Bus) Disabled() bool { return b.off }
