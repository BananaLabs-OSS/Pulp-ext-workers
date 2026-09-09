# PostgreSQL effect receipt store

Production hosts can bind notification and status-signal receipts to
`NewMigratingPostgresEffectStoreFactory`. The factory accepts an existing,
exact-scope `*sql.DB` resolver; it never reads a DSN and never owns pool
lifecycle. Sessions supplies `Pulp-ext-postgres.ExistingConnection` when
`DB_DIALECT=postgres`.

The store creates `pulp_worker_effect_receipts_v1`. Its primary key contains
the complete Pulp routing identity, effect family, and idempotency key. Pending
work is claimed with an expiring lease and monotonic fence. Only the current
claim owner/fence can commit a terminal receipt. Provider calls must still use
the intent's stable idempotency key because no local database can make an
unknown-result network call globally exactly-once.

At first construction, the migrating factory imports the matching legacy
MessagePack receipt file. A completed import renames that file with the suffix
`.postgres-migrated-v1`; it is preserved for audit/recovery but cannot be
silently loaded as stale authority on restart. Notification and status-signal
families have distinct hashed source and archive paths.

Local and test hosts whose `DB_DIALECT` is not `postgres` retain the existing
file-backed implementation. A production rollback to files after migration
requires an explicit PostgreSQL-to-file export; restoring the archived file
alone is unsafe because it may predate later terminal receipts.
