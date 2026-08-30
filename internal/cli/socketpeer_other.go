//go:build !linux

package cli

import "net"

// peerUID cannot answer away from Linux. SO_PEERCRED is a Linux socket option,
// and the BSD and macOS spelling (LOCAL_PEERCRED) is not in the standard
// library, so there is nothing here to ask the kernel with. The caller keeps
// the file check it made before dialing, and the window between that check and
// this connection stays open on these platforms.
func peerUID(net.Conn) (int, bool) { return 0, false }
