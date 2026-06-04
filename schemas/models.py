from pydantic import BaseModel, Field
from typing import Literal

class SensorEvent(BaseModel):
    sensor_id: str
    type: Literal['temp', 'pressure', 'humidity', 'vibration']
    value: float
    timestamp: int = Field(description="Epoch timestamp in milliseconds")

class ClickstreamEvent(BaseModel):
    user_id: str
    page: str
    action: Literal['click', 'view', 'scroll', 'exit']
    timestamp: int
