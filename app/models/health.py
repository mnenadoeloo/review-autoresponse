from pydantic import BaseModel
from datetime import datetime
from typing import Dict

class DetailedMetrics(BaseModel):
    system: Dict[str, float]
    application: Dict[str, float]
    database: Dict[str, dict]
    api: Dict[str, dict]

class ServiceHealth(BaseModel):
    status: str
    last_check: datetime
    metrics: DetailedMetrics 