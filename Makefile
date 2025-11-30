.PHONY: venv install setup dev activate ingest ingest-courtreserve-reservations ingest-courtreserve-members ingest-courtreserve-court-availability ingest-podplay-reservations ingest-podplay-members ingest-podplay-events ingest-courtreserve-events ingest-podplay-court-availability ingest-google-reviews ingest-staging wipe-pklyn-res wipe-pklyn-cancellations wipe-events import-duprs dbt dbt-run dbt-run-staging seed seed-designer-data test-github-env-vars list-required-secrets migrate migrate-upgrade migrate-downgrade migrate-revision migrate-history

venv:
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip

install:
	.venv/bin/pip install -r requirements/all.txt

setup: venv install

dbt:
	python3 -m scripts.run_dbt ${args}

dbt-run:
	python3 -m scripts.run_dbt run --target dev --exclude tag:skip

add-skill-levels:
	python3 -m scripts.add_skill_level_to_events

seed-designer-data:
	python3 -m scripts.seed_designer_data

dev:
	pip install -r requirements/dev.txt

activate:
	@echo "Run: source .venv/bin/activate"

ingest:
	python3 -m ingestion.main all


ingest-courtreserve-reservations:
	python3 -m ingestion.main courtreserve_reservations

ingest-courtreserve-members:
	python3 -m ingestion.main courtreserve_members

ingest-podplay-reservations:
	python3 -m ingestion.main podplay_reservations

ingest-podplay-members:
	python3 -m ingestion.main podplay_members

ingest-podplay-events:
	python3 -m ingestion.main podplay_events

ingest-courtreserve-events:
	python3 -m ingestion.main courtreserve_events

ingest-podplay-court-availability:
	python3 -m ingestion.main podplay_court_availability

ingest-courtreserve-court-availability:
	python3 -m ingestion.main courtreserve_court_availability

ingest-google-reviews:
	python3 -m ingestion.main google_reviews

ingest-staging:
	@echo "Running staging ingestion pipeline..."
	@echo "Note: Ensure PG_SCHEMA_STG is set in your environment"
	make ingest-courtreserve-events
	make ingest-podplay-events
	make ingest-podplay-court-availability
	make ingest-courtreserve-court-availability
	make ingest-courtreserve-reservations
	make ingest-courtreserve-members
	make ingest-podplay-reservations
	make add-skill-levels
	python3 -m scripts.run_dbt deps
	python3 -m scripts.run_dbt run --target dev
	make seed-designer-data
	@echo "✓ Staging ingestion pipeline completed"


wipe-pklyn-res:
	python3 -m scripts.pklyn.reset_reservations ${args}


wipe-pklyn-cancellations:
	python3 -m scripts.pklyn.eset_reservation_cancellations ${args}


wipe-pklyn-non-event-cancellations:
	python3 -m scripts.pklyn.reset_reservation_cancellations ${args}

wipe-events:
	python3 -m scripts.pklyn.reset_event_summaries ${args}

import-duprs:
	python3 -m scripts.import_dupr_scores

dbt-run-staging:
	python -m dotenv -f .env run -- dbt run --select stg_reservations fct_reservations dim_clients

# Seed data
seed:
	@echo "Running all seed files (idempotent)..."
	@python3 -c "import os; from dotenv import load_dotenv; load_dotenv(); schema = os.getenv('PG_SCHEMA'); print(f'Using schema: {schema}')"
	@echo "Running seed_organizations.sql..."
	@python3 -m dotenv -f .env run -- bash -c 'psql $$PG_DSN -v schema=$$PG_SCHEMA -f seeds/seed_organizations.sql'
	@echo "Running seed_courts.sql..."
	@python3 -m dotenv -f .env run -- bash -c 'psql $$PG_DSN -v schema=$$PG_SCHEMA -f seeds/seed_courts.sql'
	@echo "Running seed_facility_details.sql..."
	@python3 -m dotenv -f .env run -- bash -c 'psql $$PG_DSN -v schema=$$PG_SCHEMA -f seeds/seed_facility_details.sql'
	@echo "✓ All seed files completed successfully"

# Alembic migrations
migrate:
	alembic ${args}

migrate-upgrade:
	alembic upgrade head

migrate-downgrade:
	alembic downgrade -1

migrate-revision:
	alembic revision --autogenerate -m "${message}"

migrate-history:
	alembic history

# GitHub Actions workflow testing
list-required-secrets:
	@echo "Querying database for required environment variables..."
	@python3 scripts/get_required_env_vars.py

test-github-env-vars:
	@echo "Testing GitHub Actions environment variable mapping..."
	@echo "This simulates what happens in GitHub Actions to map .env-style secrets"
	@echo "The script will query the database to find customer organizations"
	@echo ""
	@export GITHUB_ENV=/tmp/github_env_test.txt && \
	rm -f /tmp/github_env_test.txt && \
	python3 scripts/setup_github_env_vars.py && \
	echo "" && \
	echo "Mapped environment variables:" && \
	if [ -f /tmp/github_env_test.txt ]; then \
		cat /tmp/github_env_test.txt; \
	else \
		echo "  (No variables mapped - check that customer organizations exist in database)"; \
	fi && \
	rm -f /tmp/github_env_test.txt