DC := docker compose

.PHONY: help setup build up seed pipeline increment dashboard governance notebook ps logs down clean

help:            ## list available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

setup: build up seed pipeline dashboard  ## full install from scratch (build -> data -> pipeline -> dashboard)
	@echo "Done. Superset :8088  Dagster :3001  Data API :8000/docs"

build:           ## build the pipeline & superset images
	docker build -t lakehouse/pipeline:0.1 images/pipeline
	docker build -t lakehouse/superset:0.1 images/superset

up:              ## start all services
	@test -f .env || (cp .env.example .env && echo ">> .env created from .env.example — change the credentials!")
	$(DC) up -d --wait

seed:            ## generate initial source data (ERP, PDFs, photos, weighbridge, tickets, telemetry)
	$(DC) run --rm seed

pipeline:        ## run the pipeline now: ingest -> bronze -> silver -> gold -> trino
	$(DC) exec dagster dagster job execute -m lakehouse.orchestration -j lakehouse_pipeline

increment:       ## simulate a day of source changes (CDC demo), then run: make pipeline
	$(DC) run --rm seed python -m lakehouse.seed --increment

dashboard:       ## provision Superset datasets, charts & dashboard
	$(DC) exec dagster python -m lakehouse.superset_setup

governance:      ## row-filter & column-mask demo: same query, different users
	@for u in admin executive analyst_finance tenant_t003; do \
	  echo "== user: $$u"; \
	  $(DC) exec -T trino trino --user $$u --output-format ALIGNED --execute \
	    "SELECT tenant_id, tenant_name, pic_phone, pic_email, pic_national_id FROM delta.gold.tenant_360 ORDER BY 1 LIMIT 3"; \
	done

notebook:        ## start JupyterLab (optional, +2-3 GB RAM)
	$(DC) --profile notebook up -d jupyter

ps:              ## service status
	$(DC) ps

logs:            ## follow logs of all services
	$(DC) logs -f --tail 50

down:            ## stop services (data is kept)
	$(DC) --profile notebook down

clean:           ## stop services & DELETE ALL DATA (volumes)
	$(DC) --profile notebook --profile jobs down -v
