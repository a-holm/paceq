// Package sdnotify is a hand-written sd_notify protocol implementation.
// It sends notifications to systemd via the NOTIFY_SOCKET unixgram socket.
// All calls are no-ops when NOTIFY_SOCKET is unset (running outside systemd).
// Zero dependencies beyond the Go standard library.
package sdnotify

import (
	"net"
	"os"
	"strings"
)

// Send writes a message to systemd's notification socket. The message is sent
// as a single SOCK_DGRAM write. Returns nil if NOTIFY_SOCKET is unset.
func Send(msg string) error {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return nil
	}
	return send(addr, msg)
}

// Status reports a human-readable status update for systemctl status.
func Status(text string) error {
	return Send("STATUS=" + text)
}

// Watchdog tells systemd the process is alive. Must be sent at least once per
// WatchdogSec interval, or systemd will kill and restart the process.
func Watchdog() error {
	return Send("WATCHDOG=1")
}

// Ready tells systemd the service has finished its startup and is ready to
// serve. An optional status line appears in systemctl status.
func Ready(status string) error {
	if status == "" {
		return Send("READY=1")
	}
	return Send("READY=1\nSTATUS=" + status)
}

// Stopping tells systemd the service is shutting down. This prevents systemd
// from restarting the service during a clean stop. An optional status line
// appears in systemctl status.
func Stopping(status string) error {
	if status == "" {
		return Send("STOPPING=1")
	}
	return Send("STOPPING=1\nSTATUS=" + status)
}

func send(addr string, msg string) error {
	// Abstract namespace: systemd uses @ prefix (e.g. "@/org/freedesktop/systemd1/notify").
	// The kernel expects a NUL byte as the first character for abstract sockets.
	if strings.HasPrefix(addr, "@") {
		addr = "\x00" + addr[1:]
	}
	raddr, err := net.ResolveUnixAddr("unixgram", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialUnix("unixgram", nil, raddr)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte(msg))
	closeErr := conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}
