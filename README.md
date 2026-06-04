# Real-Time Streaming Pipeline 🚀

[![Expert Difficulty](https://img.shields.io/badge/Difficulty-9%2F10%20Expert-red)](https://github.com/yourname/streaming-pipeline)
[![Python Version](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.18-orange)](https://flink.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-black)](https://kafka.apache.org/)

A production-grade event streaming system capable of ingesting, processing, and visualising millions of events per second with sub-second latency. This architecture mirrors high-scale systems deployed at **Netflix, Uber, and Twitter**.

---

## 📖 Technical Documentation
For the full architectural deep-dive, schema designs, and expert-level implementation details, please refer to the attached:
👉 **[Project 01: Real-Time Streaming Pipeline Technical Documentation](../project01_streaming_pipeline_docs.pdf)**

---

## 🏗️ System Architecture

Data moves linearly through six layers, each independently scalable and fault-tolerant:

1.  **Event Sources**: High-throughput producers (IoT & Clickstream) generating Avro-serialized events.
2.  **Message Broker**: **Apache Kafka 3.6+** with **Confluent Schema Registry** for strict data contracts.
3.  **Stream Processing**: **Apache Flink 1.18** executing stateful windowed aggregations and metadata enrichment.
4.  **Real-Time Anomaly Detection**: Z-score based spike detection integrated into the processing stream.
5.  **Multi-Tier Sinks**:
    *   **PostgreSQL 15+**: Relational sink for historical queries and BI.
    *   **Redis 7+**: High-speed cache for real-time dashboard state.
6.  **Observability**: **Prometheus** for metrics collection and **Grafana** for live visualization.

---

## 🛠️ Technical Stack

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Language** | Python 3.11 | Primary logic & PyFlink API |
| **Stream Engine** | Apache Flink 1.18 | Stateful processing & Windows |
| **Broker** | Apache Kafka | Event ingestion & decoupling |
| **Registry** | Confluent Schema Registry | Avro schema enforcement |
| **Database** | PostgreSQL | Historical persistence |
| **Cache** | Redis | Real-time state store |
| **Monitoring** | Grafana + Prometheus | Live observability |

---

## 🚀 Getting Started

### 1. Prerequisites
*   Docker & Docker Compose (v2.0+)
*   Python 3.10+
*   Java JDK 11 (minimum)

### 2. Environment Setup
Clone the repository and initialize the local environment:
```bash
cp .env.example .env
make setup
```

### 3. Launch the Stack
Start the infrastructure and build the custom images:
```bash
make build
make up
```

### 4. Initialize Topics & Jobs
The pipeline is self-bootstrapping. Use the following scripts to finalize setup:
```bash
# Create Kafka topics with correct partitions
bash scripts/create_topics.sh

# The Flink jobs will be automatically submitted by the 'flink-job-submitter' service
# You can monitor logs to verify:
docker compose -f docker/docker-compose.yml logs -f flink-job-submitter
```

---

## 📊 Monitoring & Observability

Access the following endpoints to monitor your pipeline:

*   **Grafana Dashboards**: [http://localhost:3000](http://localhost:3000) (User: `admin` / Pass: `admin`)
    *   *Note: The 'Real-Time Streaming Pipeline' dashboard is auto-provisioned.*
*   **Flink Web UI**: [http://localhost:8081](http://localhost:8081)
*   **Prometheus**: [http://localhost:9090](http://localhost:9090)
*   **Kafdrop (Kafka UI)**: [http://localhost:9000](http://localhost:9000)

---

## 🧪 Testing Strategy

The project includes an exhaustive testing suite across multiple tiers:

*   **Unit Tests**: Validate window math and anomaly logic.
    ```bash
    pytest tests/test_aggregator.py tests/test_anomaly.py
    ```
*   **Integration Tests**: Verify end-to-end data flow (Avro → Kafka → Flink → DB).
    ```bash
    pytest tests/test_consumers.py
    ```
*   **Chaos Testing**: Simulate broker and taskmanager failures.
    ```bash
    pytest tests/test_fault_tolerance.py
    ```
*   **Load Testing**: Stress test the pipeline with Locust.
    ```bash
    locust -f tests/load_test.py --headless -u 1000 -r 100
    ```

---

## 📁 Project Structure

```text
streaming-pipeline/
├── producers/          # Avro-serialized Kafka producers
├── consumers/          # Exactly-once consumers (Raw & Alerts)
├── processors/         # PyFlink Aggregation & Enrichment jobs
├── schemas/            # Avro (.avsc) and Pydantic models
├── sinks/              # Postgres & Redis implementation logic
├── monitoring/         # Grafana provisioning & Prometheus configs
├── docker/             # Docker Compose & Service Dockerfiles
├── tests/              # Unit, Integration, Chaos, & Load tests
└── scripts/            # Automation scripts for topics and jobs
```

---

## 🛡️ Security & Reliability
This implementation follows **Expert Level** standards:
*   **Exactly-Once Semantics**: Achieved via Flink Checkpointing and Two-Phase Commit sinks.
*   **Schema Enforcement**: Prevents malformed data from reaching downstream processors.
*   **Rate Limiting & Backpressure**: Configured within Flink and the BaseProducer class.
