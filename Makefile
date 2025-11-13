.PHONY: venv install setup dev activate ingest ingest-courtreserve-reservations ingest-courtreserve-members ingest-podplay-reservations ingest-podplay-members wipe-pklyn-res wipe-pklyn-cancellations wipe-events import-duprs

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