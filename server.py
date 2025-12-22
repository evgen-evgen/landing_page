import json
import os
import sys
import sqlite3
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from datetime import datetime
import urllib.request

ROOT = Path(__file__).parent.resolve()
LOG_PATH = ROOT / "logs" / "visits.log"
DB_PATH = ROOT / "data" / "visits.db"
PORT = int(os.environ.get("PORT", "3000"))
LOGTAIL_TOKEN = os.environ.get("LOGTAIL_SOURCE_TOKEN")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")

# ensure log directory exists early
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
# ensure log file exists so you can see it immediately
LOG_PATH.touch(exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

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


def send_to_logtail(entry):
    if not LOGTAIL_TOKEN:
        return
    data = json.dumps(entry).encode("utf-8")
    req = urllib.request.Request(
        "https://in.logs.betterstack.com/",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LOGTAIL_TOKEN}",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=2)
    except Exception as err:  # pragma: no cover
        print("Logtail error:", err, file=sys.stderr)


def append_file(entry):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def write_db(entry):
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


def get_stats():
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


class Handler(SimpleHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path not in ("/api/log-visit", "/api/visit"):
            self.send_error(404, "Not found")
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        ip = self.headers.get("x-forwarded-for", "").split(",")[0].strip() or self.client_address[0]
        entry = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "method": "POST",
            "ip": ip,
            "requestPath": parsed.path,
        }
        if isinstance(payload, dict):
            entry.update(payload)
        entry.setdefault("path", "")
        entry.setdefault("referrer", "")
        entry.setdefault("userAgent", "")

        send_to_logtail(entry)
        append_file(entry)
        write_db(entry)

        self.send_response(204)
        self.end_headers()

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if parsed.path == "/api/stats":
            stats = get_stats()
            body = json.dumps(stats).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/favicon.ico":
            # avoid noisy 404 + broken pipe from favicon requests
            self.send_response(204)
            self.end_headers()
            return
        if parsed.path in ("/api/log-visit", "/api/visit"):
            params = parse_qs(parsed.query)
            entry = {
                "ts": datetime.utcnow().isoformat() + "Z",
                "method": "GET",
                "path": params.get("path", [""])[0],
                "referrer": params.get("referrer", [""])[0],
                "userAgent": params.get("ua", [""])[0],
                "event": params.get("event", [""])[0],
                "sessionId": params.get("sid", [""])[0],
                "utm": params.get("utm", [""])[0],
                "ip": self.headers.get("x-forwarded-for", "").split(",")[0].strip() or self.client_address[0],
                "requestPath": parsed.path,
            }
            send_to_logtail(entry)
            append_file(entry)
            write_db(entry)
            self.send_response(204)
            self.end_headers()
            return

        return super().do_GET()

    def log_error(self, format, *args):  # noqa: A003
        # suppress broken pipe noise
        if "Broken pipe" in format:
            return
        super().log_error(format, *args)

    def translate_path(self, path):  # serve static from ROOT
        path = urlparse(path).path
        if path == "/":
            path = "/index.html"
        full = ROOT / path.lstrip("/")
        return str(full)


if __name__ == "__main__":
    os.chdir(ROOT)
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Python server running at http://localhost:{PORT}")
    print(f"Logging visits to {LOG_PATH}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
