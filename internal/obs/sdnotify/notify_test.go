package sdnotify

import (
	"net"
	"os"
	"testing"
)

// testServer starts a unixgram listener and returns what it receives.
func testServer(t *testing.T) (socketPath string, received chan string) {
	t.Helper()
	dir := t.TempDir()
	socketPath = dir + "/notify.sock"
	addr, err := net.ResolveUnixAddr("unixgram", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		t.Fatal(err)
	}
	received = make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			received <- "ERR:" + err.Error()
			return
		}
		received <- string(buf[:n])
	}()
	t.Cleanup(func() { conn.Close() })
	return socketPath, received
}

// RED: Ready sends correct bytes.
func TestReadySendsCorrectBytes(t *testing.T) {
	socketPath, received := testServer(t)
	t.Setenv("NOTIFY_SOCKET", socketPath)

	Ready("test status")

	got := <-received
	want := "READY=1\nSTATUS=test status"
	if got != want {
		t.Errorf("Ready: got %q, want %q", got, want)
	}
}

// RED: Watchdog sends correct bytes.
func TestWatchdogSendsCorrectBytes(t *testing.T) {
	socketPath, received := testServer(t)
	t.Setenv("NOTIFY_SOCKET", socketPath)

	Watchdog()

	got := <-received
	want := "WATCHDOG=1"
	if got != want {
		t.Errorf("Watchdog: got %q, want %q", got, want)
	}
}

// RED: Stopping sends correct bytes.
func TestStoppingSendsCorrectBytes(t *testing.T) {
	socketPath, received := testServer(t)
	t.Setenv("NOTIFY_SOCKET", socketPath)

	Stopping("draining")

	got := <-received
	want := "STOPPING=1\nSTATUS=draining"
	if got != want {
		t.Errorf("Stopping: got %q, want %q", got, want)
	}
}

// RED: Abstract namespace. Set NOTIFY_SOCKET=@test and verify Send transforms it.
func TestAbstractNamespace(t *testing.T) {
	// Systemd uses @ for abstract namespace. Our Send must convert @ to \x00.
	// We prove the conversion by checking that the dial address starts with \x00.
	// We use a real abstract namespace listener.
	addr, err := net.ResolveUnixAddr("unixgram", "@paceq-sdnotify-test")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	received := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			received <- "ERR:" + err.Error()
			return
		}
		received <- string(buf[:n])
	}()

	t.Setenv("NOTIFY_SOCKET", "@paceq-sdnotify-test")
	Ready("abstract")

	got := <-received
	want := "READY=1\nSTATUS=abstract"
	if got != want {
		t.Errorf("Abstract: got %q, want %q", got, want)
	}
}

// RED: No-op without NOTIFY_SOCKET.
func TestNoopWithoutNotifySocket(t *testing.T) {
	// Ensure NOTIFY_SOCKET is unset.
	os.Unsetenv("NOTIFY_SOCKET")

	// Every function should return without error.
	if err := Ready(""); err != nil {
		t.Errorf("Ready without NOTIFY_SOCKET: %v", err)
	}
	if err := Watchdog(); err != nil {
		t.Errorf("Watchdog without NOTIFY_SOCKET: %v", err)
	}
	if err := Stopping(""); err != nil {
		t.Errorf("Stopping without NOTIFY_SOCKET: %v", err)
	}
	if err := Status("test"); err != nil {
		t.Errorf("Status without NOTIFY_SOCKET: %v", err)
	}
}

// RED: Send with empty socket returns nil.
func TestSendWithEmptySocket(t *testing.T) {
	os.Unsetenv("NOTIFY_SOCKET")
	if err := Send("READY=1"); err != nil {
		t.Errorf("Send with no socket: %v", err)
	}
}

// RED: Status sends correct bytes.
func TestStatusSendsCorrectBytes(t *testing.T) {
	socketPath, received := testServer(t)
	t.Setenv("NOTIFY_SOCKET", socketPath)

	Status("running migrations")

	got := <-received
	want := "STATUS=running migrations"
	if got != want {
		t.Errorf("Status: got %q, want %q", got, want)
	}
}
