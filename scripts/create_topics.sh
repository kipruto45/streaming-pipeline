#!/bin/bash

# Kafka broker address
BOOTSTRAP_SERVER="localhost:9092"

echo "Creating Kafka topics..."

# Create raw IoT sensor events topic
docker exec docker-kafka-1 kafka-topics --create --bootstrap-server $BOOTSTRAP_SERVER --replication-factor 1 --partitions 12 --topic raw.iot.sensor.events

# Create raw clickstream events topic
docker exec docker-kafka-1 kafka-topics --create --bootstrap-server $BOOTSTRAP_SERVER --replication-factor 1 --partitions 12 --topic raw.clickstream.events

# Create processed aggregates topic
docker exec docker-kafka-1 kafka-topics --create --bootstrap-server $BOOTSTRAP_SERVER --replication-factor 1 --partitions 6 --topic processed.aggregates

# Create alerts anomalies topic
docker exec docker-kafka-1 kafka-topics --create --bootstrap-server $BOOTSTRAP_SERVER --replication-factor 1 --partitions 3 --topic alerts.anomalies

# Create dead-letter queue topic
docker exec docker-kafka-1 kafka-topics --create --bootstrap-server $BOOTSTRAP_SERVER --replication-factor 1 --partitions 3 --topic dlq.failed.events

echo "Topics created successfully."
