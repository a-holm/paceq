//go:build !linux

package store

// checkLocalFS accepts every directory outside Linux. The refusal is built on
// the statfs magic numbers in <linux/magic.h>, which have no portable
// equivalent, and guessing from a mount table would refuse local filesystems.
func checkLocalFS(string) error {
	return nil
}
