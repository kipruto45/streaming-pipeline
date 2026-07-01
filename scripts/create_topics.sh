#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS_CONFIG="$(cd "$(dirname "$0")/.." && pwd)/config/topics.yaml"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to parse config/topics.yaml" >&2
  exit 1
fi

if ! python3 - <<'PY' "$TOPICS_CONFIG"
import sys
from pathlib import Path
import yaml

config_path = Path(sys.argv[1])
with config_path.open("r", encoding="utf-8") as handle:
    data = yaml.safe_load(handle)
print(data.get("topics", []))
PY
>/dev/null 2>&1; then
  echo "Unable to parse topic configuration from $TOPICS_CONFIG" >&2
  exit 1
fi

if ! kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
  echo "Kafka is not reachable at $BOOTSTRAP_SERVER" >&2
  exit 1
fi

python3 - <<'PY' "$TOPICS_CONFIG" "$BOOTSTRAP_SERVER"
import subprocess
import sys
from pathlib import Path
import yaml

config_path = Path(sys.argv[1])
bootstrap_server = sys.argv[2]

with config_path.open("r", encoding="utf-8") as handle:
    topics = yaml.safe_load(handle).get("topics", [])

existing = subprocess.run(
    ["kafka-topics.sh", "--bootstrap-server", bootstrap_server, "--list"],
    capture_output=True,
    text=True,
    check=True,
)
existing_topics = set(existing.stdout.splitlines())

for topic in topics:
    topic_name = topic["name"]
    partitions = topic["partitions"]
    replication_factor = topic["replication_factor"]["dev"]
    retention_ms = topic["retention_ms"]
    cleanup_policy = topic["cleanup_policy"]

    if topic_name in existing_topics:
        print(f"SKIP {topic_name} (already exists)")
        continue

    subprocess.run(
        [
            "kafka-topics.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--create",
            "--topic",
            topic_name,
            "--partitions",
            str(partitions),
            "--replication-factor",
            str(replication_factor),
            "--config",
            f"retention.ms={retention_ms}",
            "--config",
            f"cleanup.policy={cleanup_policy}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    print(f"CREATE {topic_name} partitions={partitions} rf={replication_factor} retention_ms={retention_ms}")
PY
