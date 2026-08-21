//go:build linux

package store

import (
	"fmt"
	"os"
	"strings"
)

// bootIDPath is the kernel's identifier for the current boot. It is regenerated
// on every boot, so a change means the machine restarted, which in turn means
// no process paceq started can have survived.
const bootIDPath = "/proc/sys/kernel/random/boot_id"

func readBootID() (string, error) {
	raw, err := os.ReadFile(bootIDPath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", bootIDPath, err)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("%s is empty", bootIDPath)
	}
	return value, nil
}
