package cli

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/sockpath"
)

// TestServeRefusesASocketPathOverTheKernelLimit is the flag half of #234, and
// it mirrors what --metrics-listen already promises one flag over: a surface
// the operator configured and the daemon cannot provide ends the command, it
// does not become a daemon that quietly has neither. The length is knowable
// from the string, so the refusal happens before anything starts and names the
// measurement the kernel's EINVAL leaves out.
func TestServeRefusesASocketPathOverTheKernelLimit(t *testing.T) {
	dir := newServeProject(t)
	socket := filepath.Join("/tmp", strings.Repeat("s", sockpath.MaxLen+1-len("/tmp/")))

	// The bound is what makes a regression fail rather than hang: an
	// unrefused path reaches Serve, and Serve does not return on its own.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := runCLIContext(t, ctx, dir, nil, "serve", "--socket", socket)

	if res.code != ExitUsage {
		t.Errorf("serve --socket of %d bytes: exit %d, want %d (usage)\n%s%s",
			len(socket), res.code, ExitUsage, res.stdout, res.stderr)
	}
	for _, want := range []string{strconv.Itoa(len(socket)), strconv.Itoa(sockpath.MaxLen), socket} {
		if !strings.Contains(res.stderr, want) {
			t.Errorf("the refusal does not name %q:\n%s", want, res.stderr)
		}
	}
}
