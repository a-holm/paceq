package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/a-holm/paceq/internal/clock"
)

// OpenReadOnly opens only the read pool of a database: no writer connection,
// no state lock, no migration and none of the startup writes. It exists for
// the commands that must work while the daemon holds the state lock, such as
// paceq logs, which reads files and read only database rows and nothing else.
//
// A write through the returned store fails with ErrReadOnly instead of
// reaching the database.
func OpenReadOnly(ctx context.Context, path string, opt Options) (*Store, error) {
	syncMode, err := syncMode(opt.Synchronous)
	if err != nil {
		return nil, err
	}
	if err := guardFilesystem(filepath.Dir(path), opt.AllowNetworkFS); err != nil {
		return nil, err
	}

	r, err := sql.Open(driverName, dsn(path, "mode=ro", readerSpecs(syncMode)))
	if err != nil {
		return nil, fmt.Errorf("open reader pool: %w", err)
	}
	r.SetMaxOpenConns(readerPoolSize())
	r.SetMaxIdleConns(readerPoolSize())
	r.SetConnMaxLifetime(0)
	r.SetConnMaxIdleTime(0)

	clk := opt.Clock
	if clk == nil {
		clk = clock.System()
	}
	s := &Store{r: r, path: path, clk: clk, bootID: readBootID}
	if err := verifyReaderOnly(ctx, s.r, syncMode); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// verifyReaderOnly checks every reader pragma. The writer half of
// verifyPragmas does not apply: there is no writer to verify.
func verifyReaderOnly(ctx context.Context, db *sql.DB, sync string) error {
	return verifyPool(ctx, db, "reader", readerSpecs(sync))
}
