from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime
from app.db import query_telemetry

router = APIRouter(prefix="/api", tags=["telemetry"])

@router.get("/telemetry")
async def read_telemetry(
    device_id: Optional[str] = None,
    pgn: Optional[int] = None,
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = 100
):
    data = await query_telemetry(device_id, pgn, start, end, limit)
    return {"count": len(data), "data": data}
