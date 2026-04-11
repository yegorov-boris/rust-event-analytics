# rust-event-analytics
System for collecting and analyzing user events

## E2E test data

| Period | product_a | product_b | Expected #1 |
|--------|-----------|-----------|-------------|
| 1h | 10 views (−30min) | 3 views (−30min) | **product_a** |
| 2h | 10 views (−30min) | 13 views (−30min + −90min) | **product_b** |

## Local development

run e2e tests
```bash
  make e2e-up
  ```

stop e2e tests
```bash
  make e2e-down
  ```

run performance tests
```bash
  make perf-up
```

stop performance tests
```bash
  make perf-down
```

see containers status
```bash
docker ps -a --format "table {{.Names}}\t{{.Status}}"
```

[Kafka UI](http://127.0.0.1:8081/)

[Ingestion API](http://127.0.0.1:8080/swagger-ui/)

Generate a random view event json: crates/ingestion-api/examples/random_view_event.rs

## todo:
* JWT authorization
* SASL_SSL with Kafka
