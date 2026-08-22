package daemon

import (
	"context"
	"errors"
	"sync"
)

// group is the smallest thing that does what an errgroup does here: run
// goroutines, hand them all one context, cancel that context when any of them
// fails, and report the first real failure from Wait.
//
// It exists because golang.org/x/sync is outside this repository's dependency
// budget, and the thirty lines below are all the daemon needs of it.
type group struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	errOnce sync.Once
	err     error
}

func newGroup(parent context.Context) *group {
	ctx, cancel := context.WithCancel(parent)
	return &group{ctx: ctx, cancel: cancel}
}

// goLoop runs fn with the group's context. An error that is not the group
// shutting down cancels the group, so one dead loop stops the rest instead of
// leaving a half alive daemon behind.
func (g *group) goLoop(fn func(context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn(g.ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		g.errOnce.Do(func() {
			g.err = err
			g.cancel()
		})
	}()
}

// wait blocks until every loop returned and reports the failure, if there was
// one. A stop by cancellation reports nil; the caller reads the context for
// that story.
func (g *group) wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}
