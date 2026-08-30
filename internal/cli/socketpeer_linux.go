//go:build linux

package cli

import (
	"net"
	"syscall"
)

// peerUID reads SO_PEERCRED: the uid the process on the other end of conn ran
// as when it called listen. The kernel records it at that moment, so the
// answer describes the process actually serving this connection rather than
// whatever the path pointed at a moment ago.
func peerUID(conn net.Conn) (uid int, known bool) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, false
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return 0, false
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil || credErr != nil {
		return 0, false
	}
	return int(cred.Uid), true
}
