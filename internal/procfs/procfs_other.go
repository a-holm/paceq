//go:build !linux

package procfs

// ProcStartTicks has no answer where there is no /proc. The bool is the same
// "not provably this process" signal the Linux reader gives.
func ProcStartTicks(int) (int64, bool) { return 0, false }

// BootID is empty off Linux: a caller that stores it records "unknown boot",
// which compares unequal to any future boot's value, which is the safe
// direction for a fact used to prove a machine restarted.
func BootID() string { return "" }
