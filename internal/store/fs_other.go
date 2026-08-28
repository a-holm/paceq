//go:build !linux

package store

// checkLocalFS accepts every directory outside Linux. The refusal is built on
// the statfs magic numbers in <linux/magic.h>, which have no portable
// equivalent, and guessing from a mount table would refuse local filesystems.
func checkLocalFS(string) error {
	return nil
}

// FSMagic reports zero outside Linux: the magic numbers have no portable
// equivalent, and the check degrades to "not known" rather than guessing.
func FSMagic(string) (uint64, error) { return 0, nil }

// IsNetworkFSMagic never fires outside Linux, matching checkLocalFS.
func IsNetworkFSMagic(uint64) bool { return false }
