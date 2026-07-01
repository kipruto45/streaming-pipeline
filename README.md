# Real-Time Streaming Pipeline

[![CI](https://github.com/Victor-Kipruto-Rop/streaming-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Victor-Kipruto-Rop/streaming-pipeline/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/Python-3.11-blue)](https://www.python.org/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.18-orange)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black)](https://kafka.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-ready event streaming pipeline with IoT, clickstream, and M-Pesa ingestion, Avro schema enforcement, Apache Flink processing, and resilient sink architecture.

## Overview

This repository demonstrates a complete streaming architecture with:

- Avro-encoded producers for IoT, clickstream, and M-Pesa events
- Schema Registry compatibility and evolution testing
- Kafka topic catalog with dev/prod replication settings
- PyFlink aggregation and anomaly detection
- PostgreSQL and Redis sinks for persistent and real-time data
- Dead-letter queue consumer for failed event handling
- GitHub Actions CI for linting, type checking, and tests

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

- **Flink over Spark Streaming**: true stateful per-key processing with low latency and no micro-batching overhead.
- **Avro + Schema Registry**: strict schema enforcement, compact binary payloads, and safe schema evolution.
- **Redis for real-time state**: Redis is used for sub-second dashboards while PostgreSQL stores durable historical aggregates.
- **Z-score threshold rationale**: the default `3.0` keeps false positives low for normally-distributed metrics and remains configurable.
- **DLQ isolation**: failed events are routed to a dedicated dead-letter topic and consumer, preventing pipeline blockage.

## What’s Included

- `requirements.txt` and `requirements-dev.txt`
- `pyproject.toml` with `mypy`, `black`, and `pytest` settings
- `config/topics.yaml` for Kafka topic definitions
- `scripts/create_topics.sh` for YAML-driven topic creation
- `consumers/dlq_consumer.py` with observability and graceful shutdown
- `producers/mpesa_producer.py` with realistic Kenyan transaction generation
- `schemas/` containing Avro v1/v2 and M-Pesa event schemas
- `tests/test_schema_evolution.py` for compatibility testing
- `.github/workflows/ci.yml` for CI validation
- `CHANGELOG.md` and `SECURITY.md`

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

## M-Pesa Context

This pipeline includes a dedicated M-Pesa transaction stream to model Kenyan FinTech behavior. It supports:

- realistic Safaricom MSISDN ranges (`2547...`, `2541...`)
- business-hours spikes
- P2P, Paybill, Till, withdrawal, and deposit transaction types
- failure reason handling and short retention for sensitive data

## Schema Evolution

The `schemas/` folder contains versioned Avro definitions. `tests/test_schema_evolution.py` verifies backward and forward compatibility.

## Benchmarks

Run `make load-test` to reproduce locally.

| Scenario | Events/sec | P99 Latency | Notes |
| --- | ---: | ---: | --- |
| IoT + clickstream | 1000 | < 150 ms | Local Docker deployment |
| M-Pesa burst | 500 | < 200 ms | Business-hours spike profile |

## Monitoring

- Grafana dashboards in `monitoring/` with M-Pesa throughput, status, and DLQ panels
- Prometheus scrapes the DLQ consumer and M-Pesa producer metrics
- DLQ consumer metrics available on port `8002`
- M-Pesa producer metrics available on port `8003`

## Repository Topics

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
