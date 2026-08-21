// Package doctor holds the health checks behind the doctor command: what is
// wrong with an installation, and what the operator can run about it.
//
// A check reads state and returns a Finding. It never repairs anything on its
// own, because every repair a check could make is either unnecessary or
// expensive enough that an operator has to choose the moment.
package doctor

import (
	"context"
	"fmt"

	"github.com/a-holm/paceq/internal/store"
)

// Level is how much attention a finding needs.
type Level int

const (
	// OK is a check that found what it expected.
	OK Level = iota
	// Warn is a state that works today and costs later.
	Warn
	// Fail is a broken installation, or a check that could not answer.
	Fail
)

func (l Level) String() string {
	switch l {
	case OK:
		return "ok"
	case Warn:
		return "warn"
	case Fail:
		return "fail"
	default:
		return fmt.Sprintf("unknown(%d)", int(l))
	}
}

// Finding is one check's result. Next holds commands, in the order they are
// meant to be run, so a report can be acted on without reading the source.
type Finding struct {
	Level  Level
	Title  string
	Detail string
	Next   []string
}

// AutoVacuumReader is the part of the store this check needs. Narrowing it to
// one method keeps the check runnable against a database created before paceq
// set the mode, which is the case worth testing.
type AutoVacuumReader interface {
	AutoVacuum(ctx context.Context) (store.AutoVacuumMode, error)
}

// CheckAutoVacuum reports whether the database can ever give disk back.
//
// The mode is decided when the database file is created and cannot be changed
// afterwards without a full VACUUM, so a database left at NONE grows for as
// long as it lives, whatever retention deletes. That is worth a warning on
// every start, not a note in the documentation.
func CheckAutoVacuum(ctx context.Context, db AutoVacuumReader) Finding {
	mode, err := db.AutoVacuum(ctx)
	if err != nil {
		return Finding{
			Level:  Fail,
			Title:  "auto_vacuum",
			Detail: fmt.Sprintf("could not read the setting: %v", err),
		}
	}
	if mode == store.AutoVacuumIncremental {
		return Finding{
			Level:  OK,
			Title:  "auto_vacuum",
			Detail: mode.String(),
		}
	}
	return Finding{
		Level: Warn,
		Title: "auto_vacuum",
		Detail: fmt.Sprintf("%s: this database never releases disk after retention deletes rows, "+
			"because the mode was fixed when the file was created", mode),
		Next: []string{
			"stop paceq: nothing else may hold the database while it is rewritten",
			"sqlite3 state.db \"PRAGMA auto_vacuum = INCREMENTAL; VACUUM;\"",
			"the rewrite takes an exclusive lock and needs free disk for a second copy of the file",
		},
	}
}
