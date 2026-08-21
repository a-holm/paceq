package store

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestLockHolderCarriesTheInstance pins the shape holderAlive parses. A holder
// is a host, the boot it ran on and its process id, and each part is load
// bearing.
func TestLockHolderCarriesTheInstance(t *testing.T) {
	holder := lockHolder("boot-one")

	host, err := os.Hostname()
	if err != nil {
		t.Skipf("no hostname on this machine: %v", err)
	}
	want := host + "/boot-one/" + strconv.Itoa(os.Getpid())
	if holder != want {
		t.Errorf("lockHolder is %q, want %q", holder, want)
	}
	if !holderAlive(holder, "boot-one") {
		t.Error("this process reads as dead to holderAlive, so a live migrator would be run over")
	}
}

// TestHolderOnAnotherBootIsDead is what the boot id buys the migration lock. A
// process id is recycled on every boot, so a lock row from before a restart
// names a pid that may well exist again. Without the boot id the next start
// waits out the whole lock lifetime after every machine restart.
func TestHolderOnAnotherBootIsDead(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Skipf("no hostname on this machine: %v", err)
	}
	// Our own pid: alive by any process check, and the only thing that can tell
	// the truth here is that the boot id differs.
	holder := host + "/boot-before-the-restart/" + strconv.Itoa(os.Getpid())

	if holderAlive(holder, "boot-after-the-restart") {
		t.Error("a lock holder from an earlier boot reads as alive, so a restart waits out the lock for nothing")
	}
	if !holderAlive(holder, "boot-before-the-restart") {
		t.Error("a lock holder from this boot reads as dead")
	}
}

// TestHolderAliveFallsBackToTheProcessCheck covers the cases where the boot id
// says nothing: a holder written by a build without one, and a machine that
// cannot read one. Then the process check is all there is.
func TestHolderAliveFallsBackToTheProcessCheck(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Skipf("no hostname on this machine: %v", err)
	}
	self := strconv.Itoa(os.Getpid())

	cases := []struct {
		name   string
		holder string
		boot   string
		want   bool
	}{
		{name: "holder without a boot id", holder: host + "/" + self, boot: "boot-one", want: true},
		{name: "no boot id on this machine", holder: host + "/boot-one/" + self, boot: "", want: true},
		{name: "another host", holder: "other-host/boot-one/1", boot: "boot-two", want: true},
		{name: "not a holder shape", holder: "something-else", boot: "boot-one", want: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := holderAlive(c.holder, c.boot); got != c.want {
				t.Errorf("holderAlive(%q, %q) is %v, want %v", c.holder, c.boot, got, c.want)
			}
		})
	}
}

// TestMigrationLockHolderNamesTheBoot ties the parsing above to the row that is
// actually written, so a change to one has to change the other.
func TestMigrationLockHolderNamesTheBoot(t *testing.T) {
	s := sessionStore(t, nil, constantBoot("boot-one"))

	var holder string
	err := s.r.QueryRowContext(t.Context(),
		"SELECT holder FROM schema_migration_lock WHERE id = 1").Scan(&holder)
	if err == nil {
		t.Fatalf("the migration lock row outlived a finished migration: %q", holder)
	}

	if got := lockHolder(s.bootIdentity()); !strings.Contains(got, "boot-one") {
		t.Errorf("lock holder %q does not carry the boot id", got)
	}
}
