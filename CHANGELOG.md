# Changelog

## v0.2.0 - 2026-07-01

### Added
- M-Pesa producer for Kenyan FinTech transaction streaming
- DLQ consumer with Prometheus metrics and graceful shutdown
- Avro schema v2 with backward/forward compatibility coverage
- CI pipeline for linting, type checking, unit tests, integration tests, and Docker builds
- Topic configuration for the full Kafka topic catalog
- Schema evolution regression tests

### Changed
- Split production and development dependencies
- Upgraded environment example and Makefile targets for local and CI use

## v0.1.0 - Initial release

### Added
- IoT and clickstream producers
- Flink aggregation and anomaly detection jobs
- PostgreSQL and Redis sinks
- Grafana and Prometheus observability

## Roadmap

- [ ] Kafka SASL/SSL configuration for production deployment
- [ ] Helm chart for Kubernetes deployment
- [ ] dbt models for historical aggregate reporting on the PostgreSQL sink
- [ ] Airflow DAG for daily data quality checks on processed.aggregates
- [ ] Flink SQL layer for ad-hoc streaming queries
