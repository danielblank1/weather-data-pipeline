# ============================================================
# Weather Data Pipeline — Makefile
# ============================================================

.PHONY: help up down restart logs ingest dbt spark kafka-produce kafka-consume bruin clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

up: ## Start all services
	docker compose up -d
	@echo "\n✓ Services starting. Access points:"
	@echo "  Kestra UI:     http://localhost:8080"
	@echo "  Spark Master:  http://localhost:8082"
	@echo "  Metabase:      http://localhost:3000"
	@echo "  PostgreSQL:    localhost:5432"

down: ## Stop all services
	docker compose down

restart: ## Restart all services
	docker compose down && docker compose up -d

logs: ## Follow logs for all services
	docker compose logs -f

logs-%: ## Follow logs for a specific service (e.g., make logs-postgres)
	docker compose logs -f $*

# ---------------------------------------------------------------------------
# Data Pipeline
# ---------------------------------------------------------------------------

ingest: ## Run GHCN data ingestion (default: 2024-2025)
	cd ingestion && pip install -r requirements.txt && \
	python ingest_ghcn.py --years 2024 2025

ingest-backfill: ## Backfill 10 years of data (2015-2025)
	cd ingestion && pip install -r requirements.txt && \
	python ingest_ghcn.py --years 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025

dbt: ## Run dbt models
	cd dbt/weather_dbt && dbt deps && dbt run

dbt-test: ## Run dbt tests
	cd dbt/weather_dbt && dbt test

dbt-docs: ## Generate and serve dbt docs
	cd dbt/weather_dbt && dbt docs generate && dbt docs serve --port 8085

spark: ## Submit Spark batch job
	docker exec weather_spark_master \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-jobs/weather_features.py

spark-full: ## Submit Spark batch job with full history
	docker exec weather_spark_master \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-jobs/weather_features.py --full-history

# ---------------------------------------------------------------------------
# Bruin — Ingestion & Quality Checks
# ---------------------------------------------------------------------------

bruin-validate: ## Validate Bruin pipeline definitions
	cd bruin && bruin validate .

bruin-run: ## Run full Bruin pipeline (ingest + quality checks)
	cd bruin && bruin run .

bruin-run-docker: ## Run Bruin pipeline inside Docker
	docker compose run --rm bruin run --environment docker .

bruin-quality: ## Run only Bruin quality check assets
	cd bruin && bruin run --only quality.validate_raw_daily .
	cd bruin && bruin run --only quality.validate_stations .
	cd bruin && bruin run --only quality.validate_marts .

bruin-ingest: ## Run only Bruin ingestion assets
	cd bruin && bruin run --only raw.ghcn_daily_ingest .
	cd bruin && bruin run --only raw.ghcn_stations_ingest .

# ---------------------------------------------------------------------------
# Kafka Streaming
# ---------------------------------------------------------------------------

kafka-produce: ## Start Kafka weather producer
	cd kafka && pip install -r requirements.txt && \
	python weather_producer.py --speed 100 --limit 5000

kafka-consume: ## Start Kafka weather consumer
	cd kafka && pip install -r requirements.txt && \
	python weather_consumer.py

kafka-topic: ## Create Kafka topic
	docker exec weather_kafka kafka-topics \
		--create --topic weather.observations.raw \
		--bootstrap-server localhost:29092 \
		--partitions 3 --replication-factor 1

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

dashboard-views: ## Create Metabase-friendly views in PostgreSQL
	docker exec -i weather_postgres \
		psql -U weather -d weather_warehouse < dashboard/metabase_setup.sql

# ---------------------------------------------------------------------------
# Terraform
# ---------------------------------------------------------------------------

tf-init: ## Initialize Terraform
	cd terraform && terraform init

tf-plan: ## Plan Terraform changes
	cd terraform && terraform plan

tf-apply: ## Apply Terraform changes
	cd terraform && terraform apply -auto-approve

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

psql: ## Open psql shell
	docker exec -it weather_postgres psql -U weather -d weather_warehouse

clean: ## Remove all volumes and data (DESTRUCTIVE)
	docker compose down -v
	@echo "All volumes removed."
