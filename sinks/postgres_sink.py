import psycopg2
from config.settings import settings
import structlog

log = structlog.get_logger()

class PostgresSink:
    def __init__(self):
        self.dsn = settings.POSTGRES_DSN
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.cur = self.conn.cursor()
            log.info("Connected to PostgreSQL")
            self._create_table()
        except Exception as e:
            log.error("PostgreSQL connection failed", error=str(e))

    def _create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS sensor_aggregates (
            sensor_id VARCHAR(50),
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            avg_value DOUBLE PRECISION,
            max_value DOUBLE PRECISION,
            min_value DOUBLE PRECISION,
            count INTEGER,
            PRIMARY KEY (sensor_id, window_start)
        );
        """
        self.cur.execute(query)
        self.conn.commit()

    def write(self, data: dict):
        query = """
        INSERT INTO sensor_aggregates (sensor_id, window_start, window_end, avg_value, max_value, min_value, count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sensor_id, window_start) DO UPDATE SET
            avg_value = EXCLUDED.avg_value,
            max_value = EXCLUDED.max_value,
            min_value = EXCLUDED.min_value,
            count = EXCLUDED.count;
        """
        try:
            self.cur.execute(query, (
                data['sensor_id'], data['start'], data['end'], 
                data['avg'], data['max'], data['min'], data['count']
            ))
            self.conn.commit()
        except Exception as e:
            log.error("PostgreSQL write failed", error=str(e))
            self.conn.rollback()

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()
