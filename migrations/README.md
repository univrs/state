# Database Migrations

This directory contains SQLite schema migrations for univrs-state.

## Migration Files

- `001_initial.sql` - Initial schema with kv_store table

## Migration Strategy

Migrations are numbered sequentially (001, 002, ...) and applied in order.
Each migration should be idempotent (safe to run multiple times).

The current schema version is tracked in the `meta` table under the key
`schema_version`.

## Adding New Migrations

1. Create a new file: `00N_description.sql`
2. Use `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`
3. Update the schema version at the end of the migration
4. Test thoroughly before deploying
