import argparse
import os
from pathlib import Path

import psycopg2


def _resolve_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply SQL migrations for the Courtlink ELT ingestion tables."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("PG_DSN"),
        help="Postgres DSN. Defaults to the PG_DSN environment variable.",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("PG_SCHEMA"),
        help="Target schema. Defaults to the PG_SCHEMA environment variable.",
    )
    parser.add_argument(
        "--directory",
        default=Path(__file__).resolve().parent.parent / "migrations" / "sql",
        type=Path,
        help="Directory containing .sql migration files. Defaults to migrations/sql.",
    )
    return parser.parse_args()


def _load_sql_files(directory: Path) -> list[Path]:
    if not directory.exists():
        raise FileNotFoundError(f"Migration directory not found: {directory}")
    return sorted(path for path in directory.iterdir() if path.suffix.lower() == ".sql")


def _prepare_sql(sql: str, schema: str) -> str:
    quoted_schema = f'"{schema}"'
    sql = sql.replace("{{SCHEMA}}", quoted_schema)
    sql = sql.replace("{{SCHEMA_UNQUOTED}}", schema)
    return sql


def apply_migrations(*, dsn: str, schema: str, directory: Path) -> None:
    if not dsn:
        raise ValueError("Postgres DSN must be supplied via --dsn or PG_DSN.")
    if not schema:
        raise ValueError("Target schema must be supplied via --schema or PG_SCHEMA.")

    sql_files = _load_sql_files(directory)
    if not sql_files:
        print(f"No .sql files found in {directory}. Nothing to apply.")
        return

    print(f"Connecting to database with schema '{schema}'...")
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(f'SET search_path TO "{schema}"')

            for path in sql_files:
                sql = path.read_text()
                prepared = _prepare_sql(sql, schema)
                print(f"Applying migration {path.name} ...", end=" ")
                cur.execute(prepared)
                print("done.")

    print("Migrations applied successfully.")


def main() -> None:
    args = _resolve_args()
    apply_migrations(dsn=args.dsn, schema=args.schema, directory=args.directory)


if __name__ == "__main__":
    main()

