# Real-Time Streaming Pipeline

[![CI](https://github.com/Victor-Kipruto-Rop/streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Victor-Kipruto-Rop/streaming-pipeline/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.18-orange)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black)](https://kafka.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-oriented streaming platform for IoT, clickstream, and M-Pesa transaction events. The system ingests Avro-encoded records, processes them with Apache Flink, and publishes aggregates, anomalies, and dead-letter events to downstream systems.

## Architecture

```mermaid
flowchart LR
  A[IoT Producer] --> B[Kafka: iot.sensor.events]
  C[Clickstream Producer] --> D[Kafka: clickstream.events]
  E[M-Pesa Producer] --> F[Kafka: mpesa.transactions]
  B --> G[Flink: Aggregation + Anomaly Detection]
  D --> G
  F --> G
  G --> H[Kafka: processed.aggregates]
  G --> I[Kafka: alerts.anomalies]
  G --> J[PostgreSQL]
  G --> K[Redis]
  L[Failed events] --> M[Kafka: dlq.failed.events] --> N[DLQ Consumer]
  O[Prometheus] <-- scrapes all services
  P[Grafana] --> O
  Q[Schema Registry] <-- used by all producers and consumers
```

## Key Engineering Decisions

1. Flink over Spark Streaming: Flink offers true stateful per-key processing with sub-second latency and low overhead, which is ideal for windowed aggregations and anomaly detection.
2. Avro + Schema Registry over JSON: Avro gives a compact binary format and schema enforcement, while Schema Registry makes backward and forward compatibility checks explicit.
3. Redis for real-time state vs. PostgreSQL only: PostgreSQL is excellent for durable historical analytics, but Redis provides the low-latency state required for dashboards and live counters.
4. Z-score threshold rationale: A threshold of 3.0 is a strong default for normally distributed data and translates to roughly a 0.3% false-positive rate; it remains configurable through the environment.
5. DLQ design: Poison pills and other processing failures are isolated from the main pipeline so normal traffic continues flowing while operators investigate issues.

## Benchmarks

Run `make load-test` to reproduce these results on your hardware.

| Scenario | Events/sec | P99 Latency | Notes |
| --- | ---: | ---: | --- |
| IoT + clickstream | 1000 | < 150 ms | Local Docker deployment |
| M-Pesa burst | 500 | < 200 ms | Business-hours spike profile |

## M-Pesa Pipeline

The M-Pesa producer targets the Kenyan FinTech context, where transaction volume is heavy, business-hours spikes are common, and reversal workflows require careful handling. The pipeline uses sender-level partitioning and short retention for financial data so downstream systems can react quickly while respecting data sensitivity.

## Schema Evolution

The repository includes schema versions in [schemas](schemas) and a compatibility test in [tests/test_schema_evolution.py](tests/test_schema_evolution.py). The v1 to v2 migration example shows how new optional fields can be introduced without breaking existing readers.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Java 11+

### Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
```

### Run the stack

```bash
make build
make up
bash scripts/create_topics.sh
```

### Run tests

```bash
make test
make test-unit
make test-integration
```

## Repository Topics

The repository owner can set the following GitHub topics manually:

- apache-kafka
- apache-flink
- data-engineering
- stream-processing
- python
- docker
- grafana
- fintech
- mpesa
- kenya
