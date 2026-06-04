import statistics
from dataclasses import dataclass
from typing import Optional

@dataclass
class AnomalyAlert:
    sensor_id: str
    value: float
    z_score: float
    timestamp: int

class AnomalyDetector:
    def __init__(self, window_size=100, threshold=3.0):
        self.history = {}
        self.window_size = window_size
        self.threshold = threshold

    def process(self, event: dict) -> Optional[AnomalyAlert]:
        sid, val = event['sensor_id'], event['value']
        buf = self.history.setdefault(sid, [])
        buf.append(val)
        
        if len(buf) > self.window_size:
            buf.pop(0)
            
        if len(buf) >= 10:
            mean = statistics.mean(buf)
            stdev = statistics.stdev(buf) if len(buf) > 1 else 0
            stdev = stdev or 1e-9 # Prevent division by zero
            
            z = abs((val - mean) / stdev)
            
            if z > self.threshold:
                return AnomalyAlert(sid, val, z, event['timestamp'])
                
        return None
