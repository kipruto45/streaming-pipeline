import json
import signal
import sys
import threading
import time
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaException, Message
from prometheus_client import Counter, start_http_server
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from config.settings import settings

log = structlog.get_logger()

DLQ_EVENTS_TOTAL = Counter(
    "dlq_events_total",
    "Count of dead-lettered events by source topic and error type",
    labelnames=("source_topic", "error_type"),
)
DLQ_POISON_PILL_TOTAL = Counter(
    "dlq_poison_pill_total",
    "Count of malformed DLQ messages that could not be deserialized",
)


class DLQConsumer:
    """Consume events from the dead-letter topic and surface them to observability."""

    def __init__(self) -> None:
        self.conf = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "dlq-consumer-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.consumer = Consumer(self.conf)
        self.running = True
        self._shutdown_event = threading.Event()
        self._register_signal_handlers()

    def _register_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, _frame: Any) -> None:
        log.info("Received shutdown signal", signal=signum)
        self.running = False
        self._shutdown_event.set()

    def _extract_error_reason(self, message: Message) -> str:
        headers = message.headers() or []
        for key, value in headers:
            if key == "error_reason" and value is not None:
                return value.decode("utf-8", errors="replace")
        return "unknown"

    def _process_message(self, message: Message) -> None:
        topic = message.topic() or settings.DLQ_TOPIC
        key = message.key().decode("utf-8", errors="replace") if message.key() else None
        payload = message.value()
        payload_size = len(payload) if payload is not None else 0
        error_reason = self._extract_error_reason(message)

        try:
            event_payload = json.loads(payload.decode("utf-8")) if payload else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            log.critical(
                "DLQ message could not be deserialized",
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                key=key,
                payload_size=payload_size,
            )
            DLQ_POISON_PILL_TOTAL.inc()
            self.consumer.commit(message=message, asynchronous=False)
            return

        log.error(
            "Dead-letter event received",
            topic=topic,
            partition=message.partition(),
            offset=message.offset(),
            key=key,
            error_reason=error_reason,
            payload_size=payload_size,
            payload=event_payload,
        )
        DLQ_EVENTS_TOTAL.labels(
            source_topic=event_payload.get("source_topic", "unknown"),
            error_type=error_reason,
        ).inc()
        self.consumer.commit(message=message, asynchronous=False)

    def run(self) -> None:
        start_http_server(8002)
        self.consumer.subscribe([settings.DLQ_TOPIC])
        log.info("DLQ consumer started", topic=settings.DLQ_TOPIC)

        while self.running:
            try:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    log.error("Kafka consumer error", error=str(message.error()))
                    continue
                self._process_message(message)
            except KeyboardInterrupt:
                break
            except KafkaException as exc:
                log.exception("Kafka exception in DLQ consumer", error=str(exc))
                time.sleep(1)

        self.consumer.close()


@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def run_with_retry() -> None:
    consumer = DLQConsumer()
    consumer.run()


if __name__ == "__main__":
    try:
        run_with_retry()
    except RetryError as exc:
        log.critical("DLQ consumer failed after retries", error=str(exc))
        sys.exit(1)
