package engine

import (
	"math/rand/v2"
	"testing"
)

// The jitter source is a seam like the clock: injectable, with a sane
// default. These tests hold the seam's small contract.

func TestRndSeamCreatesADefaultAndKeepsItStable(t *testing.T) {
	e := &Engine{}
	first := e.rnd()
	if first == nil {
		t.Fatal("rnd() returned nil without an injected source")
	}
	if e.rnd() != first {
		t.Error("a second call returned a different source")
	}
	first.Int64N(1024) // any draw must simply work
}

func TestRndSeamUsesTheInjectedSource(t *testing.T) {
	injected := rand.New(rand.NewPCG(67, 9))
	e := &Engine{Rnd: injected}
	if e.rnd() != injected {
		t.Error("rnd() ignored the injected source")
	}
}
