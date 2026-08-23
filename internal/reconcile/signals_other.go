//go:build !linux

package reconcile

// otherSignals is the no-op stand-in for platforms where this sweep does
// nothing. The scanner is nil there already, so these are never reached;
// they exist so the package compiles everywhere with one behaviour.
type otherSignals struct{}

func defaultSignals() Signaller { return otherSignals{} }

func (otherSignals) Term(int) error { return nil }

func (otherSignals) Kill(int) {}
