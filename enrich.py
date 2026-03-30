#!/usr/bin/env python3
"""
Einmaliger Anreicherungs-Script
================================
Ruft fuer alle Meldungen ohne Koordinaten den Detail-Endpunkt ab
und speichert lat/lng, strasse, hausNummer, plz in der DB.

Kann jederzeit unterbrochen und fortgesetzt werden - bereits
angereicherte Meldungen werden uebersprungen.

Aufruf: python enrich.py
"""

import sqlite3
import requests
import logging
import time
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "ordnungsamt.db"
API_BASE = "https://ordnungsamt.berlin.de/frontend.webservice.opendata/api/meldungen"

RATE_LIMIT = 1
BATCH_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "enrich.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def init_db(conn):
    for col, typedef in [
        ("lat", "REAL"),
        ("lon", "REAL"),
        ("hausNummer", "TEXT DEFAULT ''"),
        ("enriched", "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE meldungen ADD COLUMN {col} {typedef}")
            conn.commit()
        except sqlite3.OperationalError:
            pass


def fetch_detail(session, meldung_id):
    url = f"{API_BASE}/{meldung_id}"
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=15, headers=HEADERS)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("index", data.get("meldungen", []))
                if items:
                    return items[0]
                return {}
            elif resp.status_code == 404:
                return {}
            elif resp.status_code == 503:
                wait = 60 * (attempt + 1)
                log.warning("HTTP 503 - warte %ds (Versuch %d/3)", wait, attempt + 1)
                time.sleep(wait)
            else:
                log.warning("HTTP %d fuer ID %s", resp.status_code, meldung_id)
                return None
        except requests.exceptions.Timeout:
            log.warning("Timeout fuer ID %s (Versuch %d/3)", meldung_id, attempt + 1)
            time.sleep(30)
        except Exception as e:
            log.warning("Fehler fuer ID %s: %s", meldung_id, e)
            return None
    return None


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    todo = conn.execute("""
        SELECT id FROM meldungen
        WHERE (lat IS NULL OR enriched = 0)
        ORDER BY id ASC
    """).fetchall()

    total = len(todo)
    log.info("Zu bereichern: %d Meldungen", total)

    if total == 0:
        log.info("Alle Meldungen bereits angereichert!")
        conn.close()
        return

    session = requests.Session()
    done = 0
    errors = 0
    batch = []
    start_time = datetime.now()

    for i, row in enumerate(todo):
        mid = row["id"]
        detail = fetch_detail(session, mid)

        if detail is None:
            errors += 1
            time.sleep(10)
            continue

        lat = detail.get("lat")
        lng = detail.get("lng") or detail.get("lon")
        strasse = detail.get("strasse", "")
        haus = detail.get("hausNummer", "")
        plz = detail.get("plz", "")

        batch.append((lat, lng, strasse, haus, plz, mid))
        done += 1

        if len(batch) >= BATCH_SIZE:
            conn.executemany("""
                UPDATE meldungen
                SET lat=?, lon=?, strasse=?, hausNummer=?, plz=?, enriched=1
                WHERE id=?
            """, batch)
            conn.commit()
            batch = []

            elapsed = (datetime.now() - start_time).total_seconds()
            rate = done / elapsed if elapsed > 0 else 0
            remaining = (total - i - 1) / rate / 3600 if rate > 0 else 0
            log.info(
                "Fortschritt: %d/%d (%.1f%%) - %.1f/s - noch ca. %.1fh",
                done, total, (i + 1) / total * 100, rate, remaining
            )

        time.sleep(1.0 / RATE_LIMIT)

    if batch:
        conn.executemany("""
            UPDATE meldungen
            SET lat=?, lon=?, strasse=?, hausNummer=?, plz=?, enriched=1
            WHERE id=?
        """, batch)
        conn.commit()

    elapsed = (datetime.now() - start_time).total_seconds()
    log.info("Fertig! %d angereichert, %d Fehler in %.0f Minuten", done, errors, elapsed / 60)

    mit_coords = conn.execute("SELECT COUNT(*) FROM meldungen WHERE lat IS NOT NULL").fetchone()[0]
    log.info("Meldungen mit Koordinaten: %d / %d", mit_coords, total)
    conn.close()


if __name__ == "__main__":
    run()
