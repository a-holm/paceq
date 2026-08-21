//go:build !unix

package cli

// isTTY reports no terminal where paceq has no way to ask for one. The effect
// is that output is JSON unless -o says otherwise, which is the safe default:
// a script reading JSON works, a script reading text meant for people does not.
func isTTY(uintptr) bool { return false }
