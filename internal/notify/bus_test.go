package notify_test

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/notify"
)

// A thousand notifies against one subscriber that never drains must never
// block the sender and must never leave more than one pending wake. The bus
// coalesces on purpose (05 section 3.2): one waiting wake per subscriber is
// enough, because a wake carries no fact beyond "look again".
func TestThousandNotifiesNeverBlockAndCoalesceToOne(t *testing.T) {
	bus := notify.New()
	wake := bus.Subscribe(notify.TopicRunQueued)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			bus.Notify(notify.TopicRunQueued)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("1000 Notify calls did not finish: the bus blocked on a slow consumer")
	}

	if pending := len(wake); pending > 1 {
		t.Fatalf("%d wakes are pending after 1000 notifies, want at most 1", pending)
	}
}

// The one pending wake is real: a subscriber that drains once receives it.
func TestSubscriberReceivesTheSinglePendingWake(t *testing.T) {
	bus := notify.New()
	wake := bus.Subscribe(notify.TopicScheduleChanged)

	for i := 0; i < 50; i++ {
		bus.Notify(notify.TopicScheduleChanged)
	}

	select {
	case <-wake:
	default:
		t.Fatal("no wake arrived although the topic was notified")
	}

	// Draining does not resurrect old wakes: the channel stays at what is
	// left in it, and nothing refills it without a new notify.
	if n := len(wake); n > 1 {
		t.Fatalf("%d wakes were buffered, coalescing keeps at most 1", n)
	}
}

// Topics are separate: a wake on one topic says nothing about another.
func TestTopicsAreIndependent(t *testing.T) {
	bus := notify.New()
	runs := bus.Subscribe(notify.TopicRunQueued)
	steps := bus.Subscribe(notify.TopicStepReady)

	bus.Notify(notify.TopicRunQueued)

	select {
	case <-steps:
		t.Fatal("a notify on TopicRunQueued woke the TopicStepReady subscriber")
	default:
	}
	select {
	case <-runs:
	default:
		t.Fatal("the TopicRunQueued subscriber got no wake for its own topic")
	}
}

// Two subscribers to one topic each get their own single wake. A broadcast
// that drops one of them would make the fast path depend on subscription
// order.
func TestEverySubscriberOfATopicIsWoken(t *testing.T) {
	bus := notify.New()
	first := bus.Subscribe(notify.TopicCancelRequested)
	second := bus.Subscribe(notify.TopicCancelRequested)

	bus.Notify(notify.TopicCancelRequested)

	for name, ch := range map[string]<-chan struct{}{"first": first, "second": second} {
		select {
		case <-ch:
		default:
			t.Fatalf("subscriber %s was not woken", name)
		}
	}
}

// An unsubscribe stops the wakes and drops the slot, so a long lived daemon
// cannot grow the subscriber list forever.
func TestUnsubscribeStopsWakes(t *testing.T) {
	bus := notify.New()
	wake := bus.Subscribe(notify.TopicStepReady)
	bus.Unsubscribe(notify.TopicStepReady, wake)

	bus.Notify(notify.TopicStepReady)

	select {
	case <-wake:
		t.Fatal("an unsubscribed subscriber was still woken")
	default:
	}
}

// A disabled bus is the --no-notify-bus path: nothing is ever woken, nothing
// blocks, and subscribing still hands back a usable channel so callers need
// no second code path. The tickers alone carry correctness when this runs.
func TestDisabledBusNeverWakes(t *testing.T) {
	bus := notify.Disabled()
	wake := bus.Subscribe(notify.TopicScheduleChanged)

	for i := 0; i < 100; i++ {
		bus.Notify(notify.TopicScheduleChanged)
	}

	select {
	case <-wake:
		t.Fatal("a disabled bus woke a subscriber")
	default:
	}
}
