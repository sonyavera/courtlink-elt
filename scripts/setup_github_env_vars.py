#!/usr/bin/env python3
"""
Script to map .env-style secrets ({CLIENT_CODE}_USERNAME) to code-expected format
(CR_API_USER_{CLIENT_CODE}) for GitHub Actions.

This script queries the database to get customer organizations and maps:
- {CLIENT_CODE}_USERNAME -> CR_API_USER_{CLIENT_CODE}
- {CLIENT_CODE}_PASSWORD -> CR_API_PW_{CLIENT_CODE}
- {CLIENT_CODE}_API_KEY -> PODPLAY_API_KEY_{CLIENT_CODE}

Client codes are determined by querying court_availability_scraper.organizations.
"""
import os
import sys

try:
    import psycopg2
except ImportError:
    print(
        "Error: psycopg2 not installed. Install with: pip install psycopg2-binary",
        file=sys.stderr,
    )
    sys.exit(1)


def get_customer_organizations():
    """Query database to get customer organizations."""
    pg_dsn = os.getenv("PG_DSN")
    if not pg_dsn:
        print("Error: PG_DSN environment variable not set", file=sys.stderr)
        sys.exit(1)

    schema = os.getenv("PG_SCHEMA")
    if not schema:
        print("Error: PG_SCHEMA environment variable not set", file=sys.stderr)
        sys.exit(1)

    try:
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()

        query = f"""
        SELECT source_system_code, client_code
        FROM {schema}.organizations
        WHERE is_customer = true
        ORDER BY source_system_code, client_code
        """

        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return rows
    except Exception as e:
        print(f"Error querying database: {e}", file=sys.stderr)
        sys.exit(1)


def setup_env_vars():
    """Map client-specific secrets to the format expected by the ingestion code."""

    github_env_file = os.getenv("GITHUB_ENV")
    if not github_env_file:
        print("Error: GITHUB_ENV not set", file=sys.stderr)
        sys.exit(1)

    # Get customer organizations from database
    organizations = get_customer_organizations()

    if not organizations:
        print("Warning: No customer organizations found in database", file=sys.stderr)
        return

    mapped_vars = []

    # Process each organization
    for source_system, client_code in organizations:
        code_upper = client_code.upper()

        if source_system == "courtreserve":
            username_key = f"{code_upper}_USERNAME"
            password_key = f"{code_upper}_PASSWORD"

            username = os.getenv(username_key)
            password = os.getenv(password_key)

            if username:
                # Map to format code expects: CR_API_USER_{CLIENT_CODE}
                mapped_vars.append(f"CR_API_USER_{code_upper}={username}")

            if password:
                mapped_vars.append(f"CR_API_PW_{code_upper}={password}")

        elif source_system == "podplay":
            api_key_name = f"{code_upper}_API_KEY"
            api_key = os.getenv(api_key_name)

            if api_key:
                # Map to format code expects: PODPLAY_API_KEY_{CLIENT_CODE}
                mapped_vars.append(f"PODPLAY_API_KEY_{code_upper}={api_key}")

    # Write all mapped variables to GITHUB_ENV
    if mapped_vars:
        with open(github_env_file, "a") as f:
            for var in mapped_vars:
                f.write(f"{var}\n")
        print(f"Mapped {len(mapped_vars)} environment variables", file=sys.stderr)
    else:
        print("Warning: No environment variables were mapped", file=sys.stderr)


if __name__ == "__main__":
    setup_env_vars()
