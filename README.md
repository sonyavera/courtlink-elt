# Courtlink ELT

## Database bootstrap

Before running ingestion jobs against a new Postgres schema, apply the SQL
migrations shipped with the repo. The helper script reads `*.sql` files from
`migrations/sql` (ordered lexicographically) and runs them in a single
transaction.

```
python -m dotenv -f .env run -- python scripts/apply_migrations.py
```

The script defaults to `PG_DSN` and `PG_SCHEMA` from your environment. To target
a different schema/DSN:

```
python -m dotenv -f .env run -- python scripts/apply_migrations.py \
  --dsn "$OTHER_PG_DSN" \
  --schema my_alt_schema
```

Re-running the command is safe; the SQL uses `IF NOT EXISTS` guards so you can
bootstrap fresh schemas (e.g., dev/test) without impacting existing ones.

