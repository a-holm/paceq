# Migrations

SQL migration files live here. `internal/store/migrate.go` embeds this directory and applies the files in version order on the store's single write connection.

## Rules

- File name is `NNNN_name.sql`: four digits, an underscore, a lower case name. Anything else fails to load.
- Versions run from `0001` upwards with no gaps and no repeats. A gap means a file was lost in a merge, so loading fails rather than skipping it.
- Forward only. There are no down migrations and there never will be. A rollback restores a backup.
- A file that has been applied is immutable. Its sha256 is stored in `schema_migrations`, and a changed file makes paceq refuse to start. Fix an old migration by writing a new one.
- One migration is one transaction. The DDL, the `schema_migrations` row and `PRAGMA user_version` all commit together, so a crash leaves the migration either wholly applied or not applied at all.

## Table rebuilds

`ALTER TABLE` in SQLite only adds, renames and drops columns. Every other change means creating a new table, copying, dropping the old one and renaming, and that needs foreign keys off. `PRAGMA foreign_keys` is ignored inside a transaction, so such a migration declares itself in its first five lines:

```sql
-- +paceq rebuild
```

The engine then runs `PRAGMA foreign_keys = OFF`, the migration in one transaction, `PRAGMA foreign_key_check` (any row rolls the migration back), the commit, and `PRAGMA foreign_keys = ON`.

## Engine tables

`schema_migrations` and `schema_migration_lock` are created by the engine, not by a migration: the ledger has to exist before the first migration can record itself, and the lock has to exist before the first migration runs. Do not write them in a migration file.
