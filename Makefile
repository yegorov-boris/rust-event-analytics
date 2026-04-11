e2e-up:
	docker compose -f docker-compose-e2e.yml build ingestion-api
	docker rm -f tmp-ingestion-api 2>/dev/null; true
	docker create --name tmp-ingestion-api rust-event-analytics-ingestion-api
	docker cp tmp-ingestion-api:/ingestion-api ./target/debug/ingestion-api
	docker rm tmp-ingestion-api
	docker compose -f docker-compose-e2e.yml up -d

e2e-down:
	docker compose -f docker-compose-e2e.yml down

perf-up:
	docker compose -f docker-compose-perf.yml up -d --build

perf-down:
	docker compose -f docker-compose-perf.yml down