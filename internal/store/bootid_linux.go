//go:build linux

package store

import (
	"fmt"

	"github.com/a-holm/paceq/internal/procfs"
)

// bootIDPath is the kernel's identifier for the current boot. It is regenerated
// on every boot, so a change means the machine restarted, which in turn means
// no process paceq started can have survived.
const bootIDPath = "/proc/sys/kernel/random/boot_id"

func readBootID() (string, error) {
	value := procfs.BootID()
	if value == "" {
		return "", fmt.Errorf("%s is empty or unreadable", bootIDPath)
	}
	return value, nil
}
