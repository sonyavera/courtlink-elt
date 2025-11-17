# GitHub Actions Setup Guide

This guide explains how to set up the required environment variables (secrets) for the data ingestion workflows.

## Determining Required Secrets

To find which organizations need ETL pulls, query your database:

```sql
SELECT source_system_code, client_code, id, login_link, city, is_customer
FROM {COURT_SCRAPER_SCHEMA}.organizations
WHERE is_customer = true
ORDER BY source_system_code, client_code;
```

Replace `{COURT_SCRAPER_SCHEMA}` with your actual schema name (e.g., `court_scraper`).

## Required Secrets

Add these secrets in GitHub under **Settings → Secrets and variables → Actions → Environment secrets** (for the `prod` environment).

### Core Database Variables (Always Required)

- `PG_DSN` - PostgreSQL connection string
- `PG_SCHEMA` - PostgreSQL schema name

### Client-Specific Variables

For each customer organization found in the database query, add secrets based on their `source_system_code`:

#### CourtReserve Clients (`source_system_code = 'courtreserve'`)

For each CourtReserve client with `client_code = 'X'`, add:
- `{X}_USERNAME` - CourtReserve API username (e.g., `PKLYN_USERNAME`)
- `{X}_PASSWORD` - CourtReserve API password (e.g., `PKLYN_PASSWORD`)

**Example:** For client_code `pklyn`:
- `PKLYN_USERNAME`
- `PKLYN_PASSWORD`

#### Podplay Clients (`source_system_code = 'podplay'`)

For each Podplay client with `client_code = 'X'`, add:
- `{X}_API_KEY` - Podplay API key (e.g., `GOTHAM_API_KEY`)

**Example:** For client_code `gotham`:
- `GOTHAM_API_KEY`

### Client Code Lists

**Note:** Client codes are automatically determined by querying the `court_availability_scraper.organizations` table. You do **not** need to set `CR_CLIENT_CODES` or `PODPLAY_CLIENT_CODES` environment variables. The system will query the database to find all organizations where `is_customer = true`.

### Optional Variables

- `DEFAULT_LOOKBACK_DAYS` - Lookback days for watermark (defaults to 30 if not set)
- `INGEST_SAMPLE_SIZE` - Sample size for testing (optional)

## Example Setup

Based on the example query results:
- `courtreserve` / `pklyn` → Add `PKLYN_USERNAME`, `PKLYN_PASSWORD`
- `podplay` / `gotham` → Add `GOTHAM_API_KEY`

The system will automatically detect these clients by querying the `court_availability_scraper.organizations` table.

## How It Works

1. The workflow uses `scripts/setup_github_env_vars.py` to query the `court_availability_scraper.organizations` table for all customer organizations (`is_customer = true`).
2. For each organization found, it maps the `.env`-style secrets (`{CLIENT_CODE}_USERNAME`) to the format expected by the ingestion code (`CR_API_USER_{CLIENT_CODE}`).
3. The ingestion code also queries the database to get client codes, so no manual configuration of client lists is needed.

**Note:** You must manually add secrets for each client in the workflow file's `env` section under the "Setup environment variables" step. The workflow currently includes examples for `pklyn` and `gotham` - add more as needed when you add new customers.

## Testing Locally

Before deploying to GitHub Actions, you can test your setup locally using the Makefile targets:

### List Required Secrets

Query your database to see what secrets are needed:

```bash
make list-required-secrets
```

This will query the database and show you all required environment variables based on customer organizations.

### Test Environment Variable Mapping

Test that the environment variable mapping works correctly:

```bash
make test-github-env-vars
```

This simulates what happens in GitHub Actions to map `.env`-style secrets to the format expected by the code. The script will query the database to find customer organizations automatically.

**Note:** Make sure your `.env` file is loaded or export the required environment variables before running these tests.

## Adding New Clients

When you add a new customer organization:

1. **Add the organization to the database:**
   - Insert a new row in `court_availability_scraper.organizations` with `is_customer = true`
   - Set the `source_system_code` (`courtreserve` or `podplay`) and `client_code`

2. **Add the secrets to GitHub:**
   - For CourtReserve: Add `{CLIENT_CODE}_USERNAME` and `{CLIENT_CODE}_PASSWORD`
   - For Podplay: Add `{CLIENT_CODE}_API_KEY`

3. **Update the workflow file** (`.github/workflows/ingestion.yml`):
   - Add the new client secrets to the `env` section under the "Setup environment variables" step in both jobs

4. **Test locally** using `make test-github-workflow-setup` before deploying

The system will automatically detect the new client by querying the database - no need to set `CR_CLIENT_CODES` or `PODPLAY_CLIENT_CODES`!

