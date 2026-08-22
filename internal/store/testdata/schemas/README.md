# Schema snapshots

A full schema dump per released version goes here, named `NNNN.sql` after the `user_version` it represents.

`TestUpgradeFromEveryShippedVersion` in `internal/store` is the upgrade matrix. It slices the embedded migrations to every version below the newest, migrates the result to HEAD and compares it against a database migrated from empty, so it runs against the files that ship rather than against a snapshot. A snapshot here is what pins a schema a release wrote but the migrations no longer describe, once a release has shipped one.
