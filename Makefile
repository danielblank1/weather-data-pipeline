# ============================================================
# Weather Data Pipeline — Makefile
# Stack: DuckDB · Bruin · Streamlit · Kestra
# ============================================================

.PHONY: help up down restart logs ingest bruin clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

up: ## Start all services
	docker compose up -d
	@echo "\n✓ Services starting. Access points:"
	@echo "  Kestra UI:  http://localhost:8080"
	@echo "  Dashboard:  http://localhost:8501"

up-build: ## Rebuild images and start all services
	docker compose up -d --build

down: ## Stop all services
	docker compose down

restart: ## Restart all services
	docker compose down && docker compose up -d

logs: ## Follow logs for all services
	docker compose logs -f

logs-%: ## Follow logs for a specific service (e.g., make logs-streamlit)
	docker compose logs -f $*

# ---------------------------------------------------------------------------
# Data Pipeline
# ---------------------------------------------------------------------------

ingest: ## Ingest data (usage: make ingest STATE=AK YEARS="2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025")
	cd ingestion && pip install -r requirements.txt && \
	python ingest_ghcn.py --years $(YEARS) --state $(STATE)

ingest-country: ## Ingest by country (usage: make ingest-country COUNTRY=CA YEARS="2020 2021 2022 2023 2024 2025")
	cd ingestion && pip install -r requirements.txt && \
	python ingest_ghcn.py --years $(YEARS) --country $(COUNTRY)

ingest-city: ## Ingest by city (usage: make ingest-city CITY="Seattle, WA" RADIUS=50 YEARS="2020 2021 2022 2023 2024 2025")
	cd ingestion && pip install -r requirements.txt && \
	python ingest_ghcn.py --years $(YEARS) --city "$(CITY)" --radius $(RADIUS)

# ---------------------------------------------------------------------------
# Bruin — Transform & Quality
# ---------------------------------------------------------------------------

bruin-run: ## Run full Bruin pipeline inside Docker
	docker compose run --rm bruin run --environment docker /app/bruin

bruin-validate: ## Validate Bruin pipeline definitions
	docker compose run --rm bruin validate /app/bruin

bruin-quality: ## Run only Bruin quality check assets
	docker compose run --rm bruin run --environment docker /app/bruin --only checks

bruin-transform: ## Run only Bruin transform assets
	docker compose run --rm bruin run --environment docker /app/bruin --only main

# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------

duckdb-shell: ## Open a DuckDB shell on the data file
	docker compose run --rm bruin /bin/sh -c "duckdb /data/weather.duckdb"

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

clean: ## Remove all volumes and data (DESTRUCTIVE)
	docker compose down -v
	@echo "All volumes removed."

# ---------------------------------------------------------------------------
# Terraform
# ---------------------------------------------------------------------------

tf-init: ## Initialize Terraform
	cd terraform && terraform init

tf-plan: ## Plan Terraform changes
	cd terraform && terraform plan

tf-apply: ## Apply Terraform changes
	cd terraform && terraform apply -auto-approve
