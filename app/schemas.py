from pydantic import BaseModel, Field
from typing import Dict, Any

class TelemetryMessage(BaseModel):
    ts: int = Field(..., description="UTC timestamp in milliseconds")
    device_id: str
    pgn: int
    src: int
    dst: int
    fields: Dict[str, Any]

    @property
    def as_db_dict(self):
        return {
            "ts": self.ts,
            "device_id": self.device_id,
            "pgn": self.pgn,
            "src": self.src,
            "dst": self.dst,
            "fields": self.fields,
        }
