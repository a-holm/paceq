package doctor

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/a-holm/paceq/internal/obs"
)

// The disk-guard's offline half (#44): the same three questions the daemon's
// guard answers every thirty seconds, asked once by a human who is standing
// in front of the machine. Every check reads files and thresholds only -
// doctor never opens the database here, and every finding ends in a command
// the operator can run about it.

// DiskFunc reads free and total bytes of the filesystem holding dir. It is
// the statfs pair the daemon's guard works from; nil falls back to the
// free-only reading.
type DiskFunc func(dir string) (free, total uint64, err error)

// logDirName matches internal/logsink's LogDirName: the state directory's
// logs directory, whose children are date shards.
const logDirName = "logs"

// shardLayout matches internal/logsink and internal/janitor.
const shardLayout = "2006-01-02"

// floorOf is the binding floor: whichever of the absolute and the percentage
// floor leaves less room. It is the same rule the daemon classifies with.
func floorOf(total uint64, limits obs.DiskLimits) uint64 {
	byPct := uint64(0)
	if total > 0 {
		byPct = uint64(float64(total) * limits.MinFreePercent / 100)
	}
	if obs.UBytes(limits.MinFreeBytes) > byPct {
		return obs.UBytes(limits.MinFreeBytes)
	}
	return byPct
}

// checkDiskFloor is the disk space check with the guard's thresholds (#44).
// Under the floor, the daemon refuses new runs; that is not a broken
// installation, so the finding is a warning that says so loudly and names
// the way out. The older free-only seam keeps the old behaviour: warn under
// the shipped absolute floor, with no percentage band to compute.
func checkDiskFloor(dir string, disk DiskFunc, free FreeSpace, limits obs.DiskLimits) Finding {
	const title = "disk space"

	if disk == nil {
		bytes, err := free(dir)
		if err != nil {
			return Finding{
				Level:  Warn,
				Title:  title,
				Detail: fmt.Sprintf("could not read the free space on %s: %v", dir, err),
				Next:   []string{fmt.Sprintf("check that the path is on a mounted filesystem: df -h %s", dir)},
			}
		}
		floor := uint64(lowDisk)
		if obs.UBytes(limits.MinFreeBytes) > floor {
			floor = obs.UBytes(limits.MinFreeBytes)
		}
		if bytes < floor {
			return Finding{
				Level: Warn,
				Title: title,
				Detail: fmt.Sprintf("%s free on %s: the database, its write ahead log and the job logs "+
					"share this filesystem", byteText(bytes), dir),
				Next: []string{
					fmt.Sprintf("free space on that filesystem: df -h %s", dir),
					"or keep the state somewhere bigger: paceq --db /other/path/state.db",
				},
			}
		}
		return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("%s free on %s", byteText(bytes), dir)}
	}

	freeBytes, total, err := disk(dir)
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: fmt.Sprintf("could not read the free space on %s: %v", dir, err),
			Next:   []string{fmt.Sprintf("check that the path is on a mounted filesystem: df -h %s", dir)},
		}
	}
	floor := floorOf(total, limits)
	if freeBytes < floor {
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("%s free on %s (%s): under the floor, so the daemon is in "+
				"degraded mode and refuses new runs", byteText(freeBytes), dir, byteText(floor)),
			Next: []string{
				"free space on that filesystem: paceq prune removes expired log shards and old runs",
				"check what else is using the disk: df -h " + dir,
				"or move the state to a bigger filesystem",
			},
		}
	}
	if total > 0 && float64(freeBytes)/float64(total)*100 < limits.MinFreePercent*2 {
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("%s free on %s: inside the warning band, and paceq degrades "+
				"under %s", byteText(freeBytes), dir, byteText(floor)),
			Next: []string{
				"paceq prune  removes expired log shards and old runs before the daemon must refuse",
			},
		}
	}
	return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("%s free on %s", byteText(freeBytes), dir)}
}

// checkLogQuota compares the log directory's size with the self-imposed cap
// and names the oldest shard, so the operator knows what a prune will take
// first.
func checkLogQuota(root string, limits obs.DiskLimits) Finding {
	const title = "log directory"

	entries, err := os.ReadDir(root)
	switch {
	case os.IsNotExist(err):
		return Finding{Level: OK, Title: title, Detail: "empty: nothing has logged yet"}
	case err != nil:
		return Finding{
			Level:  Fail,
			Title:  title,
			Detail: fmt.Sprintf("could not read %s: %v", root, err),
			Next:   []string{fmt.Sprintf("check the filesystem: df -h %s", root)},
		}
	}

	var total uint64
	var shards []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, perr := time.Parse(shardLayout, e.Name()); perr != nil {
			continue
		}
		shards = append(shards, e.Name())
	}
	sort.Strings(shards)
	for _, name := range shards {
		_ = filepath.WalkDir(filepath.Join(root, name), func(_ string, entry fs.DirEntry, walkErr error) error {
			if walkErr == nil && entry.Type().IsRegular() {
				if info, statErr := entry.Info(); statErr == nil {
					total += fileSize(info)
				}
			}
			return nil
		})
	}

	detail := fmt.Sprintf("%s of run logs across %d date shards", byteText(total), len(shards))
	if obs.UBytes(limits.LogMaxBytes) > 0 && total > obs.UBytes(limits.LogMaxBytes) {
		oldest := ""
		if len(shards) > 0 {
			oldest = ", oldest shard " + shards[0]
		}
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("%s, over the %s cap%s", detail,
				byteText(obs.UBytes(limits.LogMaxBytes)), oldest),
			Next: []string{
				"paceq prune  removes expired shards; the daemon also prunes on its own once the cap is passed",
				"or lower limits.log_max_bytes in config.yaml to prune more aggressively",
			},
		}
	}
	return Finding{Level: OK, Title: title, Detail: detail}
}

// checkWAL reads the database's WAL companion file against the guard's
// levels. Growth past the warn line almost always means one long-lived read
// transaction is standing on an old snapshot and checkpointing cannot shrink
// the file - the canary 07 §6.4 sent the daemon here to say so.
func checkWAL(dbPath string, limits obs.DiskLimits) Finding {
	const title = "write ahead log"

	var size uint64
	if info, err := os.Stat(dbPath + "-wal"); err == nil {
		size = fileSize(info)
	} else if !os.IsNotExist(err) {
		return Finding{
			Level:  Fail,
			Title:  title,
			Detail: fmt.Sprintf("could not read %s-wal: %v", dbPath, err),
			Next:   []string{"check the filesystem: df -h " + filepath.Dir(dbPath)},
		}
	}

	warn := obs.UBytes(limits.WalWarnBytes)
	switch {
	case size > warn*4:
		return Finding{
			Level: Fail,
			Title: title,
			Detail: fmt.Sprintf("%s, over the error level (%s): a long-lived read transaction "+
				"is probably blocking checkpointing", byteText(size), byteText(warn*4)),
			Next: []string{
				"find what reads the database and holds it: the WAL shrinks at the next checkpoint after the reader closes",
				"paceq doctor  confirms the file is shrinking once the reader is gone",
			},
		}
	case size > warn:
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("%s, over the warning level (%s): checkpointing may be blocked "+
				"by a reader", byteText(size), byteText(warn)),
			Next: []string{
				"watch whether it keeps growing: paceq doctor in a few minutes",
				"the daemon checkpoints on its own when no run is active",
			},
		}
	default:
		return Finding{Level: OK, Title: title, Detail: byteText(size)}
	}
}
