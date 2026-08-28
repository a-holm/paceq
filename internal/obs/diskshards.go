package obs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// dateShardLayout matches internal/logsink and internal/janitor: one
// directory per UTC day under the log root, which is what makes byte-cap
// deletion a RemoveAll of whole directories instead of a query.
const dateShardLayout = "2006-01-02"

// dateShards lists the date-named children of the log root in
// chronological order and returns the index of the newest one. Names that
// do not parse as dates are left alone: an unknown name in the log root is
// someone's file until proven otherwise, which is the janitor's rule too.
// A missing log root is not an error - nothing has logged yet.
func dateShards(root string) (names []string, newest int, err error) {
	ents, err := os.ReadDir(root)
	if err != nil {
		if err == fs.ErrNotExist {
			return nil, -1, nil
		}
		return nil, -1, err
	}
	for _, e := range ents {
		if _, perr := time.Parse(dateShardLayout, e.Name()); perr != nil || !e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, len(names) - 1, nil
}

// dirSize sums the regular files of one date shard under root.
func dirSize(root, name string) (int64, bool) {
	var total int64
	err := filepath.WalkDir(filepath.Join(root, name), func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil // a file vanishing mid-walk is not a wrong total
		}
		if entry.Type().IsRegular() {
			if info, statErr := entry.Info(); statErr == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if err != nil {
		return 0, false
	}
	return total, true
}

// removeShard deletes one date shard directory whole.
func removeShard(root, name string) error {
	if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
		return fmt.Errorf("remove %s: %w", filepath.Join(root, name), err)
	}
	return nil
}
