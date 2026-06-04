import redis
import json
from config.settings import settings
import structlog

log = structlog.get_logger()

class RedisSink:
    def __init__(self):
        self.url = settings.REDIS_URL
        self.client = None

    def connect(self):
        try:
            self.client = redis.from_url(self.url)
            log.info("Connected to Redis")
        except Exception as e:
            log.error("Redis connection failed", error=str(e))

    def write_latest(self, sensor_id: str, data: dict):
        try:
            # Store latest value for real-time dashboards
            self.client.hset("sensor:latest", sensor_id, json.dumps(data))
            # Also push to a list for time-series view if needed
            self.client.lpush(f"sensor:history:{sensor_id}", json.dumps(data))
            self.client.ltrim(f"sensor:history:{sensor_id}", 0, 99) # Keep last 100
        except Exception as e:
            log.error("Redis write failed", error=str(e))

    def close(self):
        if self.client:
            self.client.close()
