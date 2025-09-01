import os, json, asyncpg
from typing import Optional, List, Dict
pool: asyncpg.Pool | None = None

async def connect_db():
    global pool
    dsn = os.getenv("DATABASE_URL")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)

async def close_db():
    if pool:
        await pool.close()

async def insert_telemetry(msg: Dict, topic: str):
    async with pool.acquire() as con:
        await con.execute(
            """
            INSERT INTO telemetry (ts, device_id, pgn, src, dst, fields, ingest_ts, broker_topic)
            VALUES (to_timestamp($1/1000.0), $2, $3, $4, $5, $6::jsonb, now(), $7)
            """,
            msg["ts"], msg["device_id"], msg["pgn"], msg["src"], msg["dst"],
            json.dumps(msg["fields"]), topic
        )

# async def insert_dlq(payload: str, topic: str, error: str):
async def insert_dlq(payload: bytes, topic: str, error: str):
    
    async with pool.acquire() as con:
        await con.execute(
            # "INSERT INTO dlq (broker_topic, payload, error) VALUES ($1, $2::jsonb, $3)",
            "INSERT INTO dlq (broker_topic, payload, error) VALUES ($1, $2, $3)",

            topic, payload, error
        )

async def query_telemetry(device_id: Optional[str], pgn: Optional[int],
                          start, end, limit: int = 100) -> List[dict]:
    clauses, params = [], []
    if device_id:
        params.append(device_id); clauses.append(f"device_id = ${len(params)}")
    if pgn is not None:
        params.append(pgn); clauses.append(f"pgn = ${len(params)}")
    if start:
        params.append(start); clauses.append(f"ts >= ${len(params)}")
    if end:
        params.append(end); clauses.append(f"ts <= ${len(params)}")

    sql = "SELECT ts, device_id, pgn, src, dst, fields FROM telemetry"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    params.append(limit)
    sql += f" ORDER BY ts DESC LIMIT ${len(params)}"

    async with pool.acquire() as con:
        rows = await con.fetch(sql, *params)
    return [dict(r) for r in rows]
