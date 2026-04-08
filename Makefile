e2e-up:
	docker compose -f docker-compose-e2e.yml up -d --build

e2e-down:
	docker compose -f docker-compose-e2e.yml down

perf-up:
	docker compose -f docker-compose-perf.yml up -d --build

perf-down:
	docker compose -f docker-compose-perf.yml down