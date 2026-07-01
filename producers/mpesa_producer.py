import random
import time
import uuid
from datetime import datetime
from pathlib import Path

import structlog
from prometheus_client import Counter

from producers.base_producer import BaseProducer

log = structlog.get_logger()

MPESA_EVENTS_PRODUCED_TOTAL = Counter(
    "mpesa_events_produced_total",
    "Count of generated M-Pesa events by transaction type and status",
    labelnames=("transaction_type", "status"),
)


class MPesaProducer(BaseProducer):
    """Generate realistic Kenyan M-Pesa transaction events.

    Assumes BaseProducer provides standard Kafka + Schema Registry setup.
    """

    def __init__(self, events_per_second: int = 50, run_duration_seconds: int | None = None):
        super().__init__(
            topic="mpesa.transactions",
            schema_file=str(Path(__file__).resolve().parents[1] / "schemas" / "mpesa_transaction.avsc"),
        )
        self.events_per_second = events_per_second
        self.run_duration_seconds = run_duration_seconds
        self._start_time = None

    def _pick_amount_kes(self) -> float:
        if random.random() < 0.8:
            return round(random.uniform(10.0, 4999.0), 2)
        return round(random.uniform(5000.0, 150000.0), 2)

    def _pick_transaction_type(self) -> str:
        return random.choices(
            population=["P2P", "PAYBILL", "TILL", "WITHDRAWAL", "DEPOSIT"],
            weights=[45, 30, 15, 7, 3],
            k=1,
        )[0]

    def _pick_status(self) -> str:
        return random.choices(
            population=["COMPLETED", "FAILED", "PENDING"],
            weights=[94, 4, 2],
            k=1,
        )[0]

    def _pick_msisdn(self) -> str:
        prefixes = ["2547", "2541"]
        return f"{random.choice(prefixes)}{random.randint(100000000, 999999999)}"

    def _pick_receiver_identifier(self, transaction_type: str) -> str:
        if transaction_type in {"PAYBILL", "TILL"}:
            return str(random.randint(100000, 999999))
        return self._pick_msisdn()

    def _pick_receiver_type(self, transaction_type: str) -> str:
        if transaction_type == "PAYBILL":
            return "PAYBILL"
        if transaction_type == "TILL":
            return "TILL"
        if transaction_type == "P2P":
            return "MSISDN"
        return "AGENT"

    def _pick_channel(self) -> str:
        return random.choices(population=["USSD", "APP", "API", "AGENT"], weights=[20, 55, 15, 10], k=1)[0]

    def generate_event(self) -> dict:
        transaction_type = self._pick_transaction_type()
        status = self._pick_status()
        now = datetime.utcnow()
        is_business_hour = 8 <= now.hour <= 18
        if is_business_hour:
            time.sleep(0.0)
        event = {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            "transaction_type": transaction_type,
            "amount_kes": self._pick_amount_kes(),
            "sender_msisdn": self._pick_msisdn(),
            "receiver_identifier": self._pick_receiver_identifier(transaction_type),
            "receiver_type": self._pick_receiver_type(transaction_type),
            "channel": self._pick_channel(),
            "status": status,
            "network_latency_ms": None if status == "COMPLETED" else random.randint(50, 600),
            "failure_reason": None if status != "FAILED" else random.choice(["INSUFFICIENT_FUNDS", "NETWORK_ERROR"]),
        }
        MPESA_EVENTS_PRODUCED_TOTAL.labels(transaction_type=transaction_type, status=status).inc()
        return event

    def run(self, rate_limit_ms: float | None = None) -> None:
        self._start_time = time.time()
        rate_limit_ms = rate_limit_ms or (1.0 / max(self.events_per_second, 1))
        log.info(
            "Starting M-Pesa producer",
            topic=self.topic,
            events_per_second=self.events_per_second,
            run_duration_seconds=self.run_duration_seconds,
        )
        try:
            while True:
                if self.run_duration_seconds is not None and (time.time() - self._start_time) >= self.run_duration_seconds:
                    break
                event = self.generate_event()
                if event:
                    self.produce(event)
                time.sleep(rate_limit_ms)
        except KeyboardInterrupt:
            log.info("M-Pesa producer stopped")
        finally:
            self.flush()


if __name__ == "__main__":
    producer = MPesaProducer(events_per_second=50)
    producer.run()
