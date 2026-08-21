# Schema snapshots

A full schema dump per released version goes here, named `NNNN.sql` after the `user_version` it represents.

`TestMigrateFromEveryHistoricalVersion` in `internal/store` is the upgrade matrix these feed: load a snapshot into an empty database, migrate to the newest version, compare the result against a database migrated from empty. Today the matrix runs on migration fixtures, because no release has shipped a schema yet.
