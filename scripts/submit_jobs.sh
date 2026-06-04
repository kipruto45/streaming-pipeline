#!/bin/bash
set -e

echo "Waiting for Flink JobManager to be ready..."
until curl -s http://flink-jobmanager:8081/overview > /dev/null; do
  sleep 5
done

echo "Submitting Windowed Aggregator Job..."
flink run -d -py /app/processors/windowed_aggregator.py

echo "Submitting Enrichment Job..."
flink run -d -py /app/processors/enrichment_job.py

echo "Jobs submitted successfully."
