import json
import os
import sys
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from urllib import request as urlrequest

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).parent.resolve()
LOG_PATH = ROOT / "logs" / "visits.log"
DB_PATH = ROOT / "data" / "visits.db"
LOGTAIL_TOKEN = os.environ.get("LOGTAIL_SOURCE_TOKEN")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.touch(exist_ok=True)

DB_LOCK = threading.Lock()
PG_CONN = None
SQLITE_CONN = None

try:
    if POSTGRES_DSN:
        import psycopg  # type: ignore

        PG_CONN = psycopg.connect(POSTGRES_DSN, autocommit=True)
        with PG_CONN.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS visits (
                    id SERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ,
                    method TEXT,
                    ip TEXT,
                    requestPath TEXT,
                    path TEXT,
                    referrer TEXT,
                    userAgent TEXT,
                    event TEXT,
                    sessionId TEXT,
                    sessionStarted BIGINT,
                    utm TEXT,
                    payload JSONB
                )
                """
            )
except Exception as err:  # pragma: no cover
    print("Postgres init error, falling back to sqlite:", err, file=sys.stderr)
    PG_CONN = None


def init_sqlite():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            method TEXT,
            ip TEXT,
            requestPath TEXT,
            path TEXT,
            referrer TEXT,
            userAgent TEXT,
            event TEXT,
            sessionId TEXT,
            sessionStarted INTEGER,
            utm TEXT,
            payload TEXT
        )
        """
    )
    conn.commit()
    return conn


if PG_CONN is None:
    SQLITE_CONN = init_sqlite()


def send_to_logtail(entry: Dict[str, Any]):
    if not LOGTAIL_TOKEN:
        return
    try:
        data = json.dumps(entry).encode("utf-8")
        req = urlrequest.Request(
            "https://in.logs.betterstack.com/",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LOGTAIL_TOKEN}",
            },
        )
        urlrequest.urlopen(req, timeout=2)
    except Exception as err:  # pragma: no cover
        print("Logtail error:", err, file=sys.stderr)


def append_file(entry: Dict[str, Any]):
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as err:  # pragma: no cover
        print("File log error:", err, file=sys.stderr)


def write_db(entry: Dict[str, Any]):
    try:
        payload_json = json.dumps(entry, ensure_ascii=False)
        if PG_CONN:
            with PG_CONN.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO visits (
                        ts, method, ip, requestPath, path, referrer,
                        userAgent, event, sessionId, sessionStarted, utm, payload
                    ) VALUES (to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        entry.get("ts", ""),
                        entry.get("method", ""),
                        entry.get("ip", ""),
                        entry.get("requestPath", ""),
                        entry.get("path", ""),
                        entry.get("referrer", ""),
                        entry.get("userAgent", ""),
                        entry.get("event", ""),
                        entry.get("sessionId", ""),
                        entry.get("sessionStarted", 0),
                        entry.get("utm", ""),
                        payload_json,
                    ),
                )
        else:
            with DB_LOCK:
                SQLITE_CONN.execute(
                    """
                    INSERT INTO visits (
                        ts, method, ip, requestPath, path, referrer,
                        userAgent, event, sessionId, sessionStarted, utm, payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.get("ts", ""),
                        entry.get("method", ""),
                        entry.get("ip", ""),
                        entry.get("requestPath", ""),
                        entry.get("path", ""),
                        entry.get("referrer", ""),
                        entry.get("userAgent", ""),
                        entry.get("event", ""),
                        entry.get("sessionId", ""),
                        entry.get("sessionStarted", 0),
                        entry.get("utm", ""),
                        payload_json,
                    ),
                )
                SQLITE_CONN.commit()
    except Exception as err:  # pragma: no cover
        print("DB write error:", err, file=sys.stderr)


def get_stats() -> Dict[str, Any]:
    today = datetime.utcnow().date().isoformat()
    stats = {
        "totalEvents": 0,
        "visits": 0,
        "clicks": 0,
        "exits": 0,
        "uniqueSessions": 0,
        "visitsToday": 0,
        "clicksToday": 0,
    }
    try:
        if PG_CONN:
            with PG_CONN.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM visits")
                stats["totalEvents"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='visit'")
                stats["visits"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='click'")
                stats["clicks"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='exit'")
                stats["exits"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT sessionId) FROM visits")
                stats["uniqueSessions"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='visit' AND ts::date = current_date")
                stats["visitsToday"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='click' AND ts::date = current_date")
                stats["clicksToday"] = cur.fetchone()[0]
        else:
            with DB_LOCK:
                cur = SQLITE_CONN.cursor()
                cur.execute("SELECT COUNT(*) FROM visits")
                stats["totalEvents"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='visit'")
                stats["visits"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='click'")
                stats["clicks"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='exit'")
                stats["exits"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(DISTINCT sessionId) FROM visits")
                stats["uniqueSessions"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='visit' AND ts LIKE ?", (f"{today}%",))
                stats["visitsToday"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM visits WHERE event='click' AND ts LIKE ?", (f"{today}%",))
                stats["clicksToday"] = cur.fetchone()[0]
    except Exception as err:  # pragma: no cover
        print("Stats error:", err, file=sys.stderr)
    return stats


def persist(entry: Dict[str, Any]):
    send_to_logtail(entry)
    append_file(entry)
    write_db(entry)


app = FastAPI()


@app.get("/health")
def health():
    return PlainTextResponse("ok")


@app.get("/api/stats")
def stats():
    return JSONResponse(get_stats())


@app.post("/api/visit")
async def visit_post(req: Request):
    try:
        payload = await req.json()
    except Exception:
        payload = {}
    entry = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "method": "POST",
        "ip": req.client.host if req.client else "",
        "requestPath": str(req.url.path),
    }
    if isinstance(payload, dict):
        entry.update(payload)
    entry.setdefault("path", "")
    entry.setdefault("referrer", "")
    entry.setdefault("userAgent", "")
    persist(entry)
    return Response(status_code=204)


@app.get("/api/visit")
def visit_get(request: Request, path: str = "", referrer: str = "", ua: str = "", event: str = "", sid: str = "", utm: str = ""):
    entry = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "method": "GET",
        "path": path,
        "referrer": referrer,
        "userAgent": ua,
        "event": event,
        "sessionId": sid,
        "utm": utm,
        "ip": request.client.host if request.client else "",
        "requestPath": str(request.url.path),
    }
    persist(entry)
    return Response(status_code=204)


# Serve static files (index.html, assets)
app.mount("/", StaticFiles(directory=ROOT, html=True), name="static")
