#!/usr/bin/env python3
"""
Einmaliges Script: Korrigiert das datum-Feld in der DB.
Liest erstellungsDatum aus meldungen.json und speichert
es korrekt als YYYY-MM-DD in der DB.

Aufruf: python fix_datum.py
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "ordnungsamt.db"
JSON_PATH = Path(__file__).parent / "meldungen.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


def parse_datum(s):
    if not s:
        return None
    for fmt in ("%d.%m.%Y - %H:%M:%S", "%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:len(fmt)+2].strip(), fmt).strftime("%Y-%m-%d")
        except:
            pass
    return None


def run():
    log.info("Lade meldungen.json...")
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    meldungen = data if isinstance(data, list) else data.get("index", data.get("meldungen", []))
    log.info("%d Meldungen geladen", len(meldungen))

    conn = sqlite3.connect(DB_PATH)

    updates = []
    skipped = 0
    for m in meldungen:
        mid = str(m.get("id", ""))
        raw = m.get("erstellungsDatum") or m.get("datum") or ""
        datum = parse_datum(raw)
        if datum and mid:
            updates.append((datum, mid))
        else:
            skipped += 1

    log.info("Aktualisiere %d Datums-Felder (%d übersprungen)...", len(updates), skipped)

    # In Batches updaten
    batch_size = 1000
    done = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        conn.executemany("UPDATE meldungen SET datum=? WHERE id=?", batch)
        conn.commit()
        done += len(batch)
        log.info("  %d / %d aktualisiert...", done, len(updates))

    # Prüfen
    sample = conn.execute("SELECT id, datum FROM meldungen WHERE datum != '2026-03-29' LIMIT 5").fetchall()
    log.info("Beispiel-Daten nach Update:")
    for r in sample:
        log.info("  %s | %s", r[0], r[1])

    wrong = conn.execute("SELECT COUNT(*) FROM meldungen WHERE datum = '2026-03-29'").fetchone()[0]
    log.info("Noch falsches Datum (2026-03-29): %d", wrong)

    conn.close()
    log.info("Fertig!")


if __name__ == "__main__":
    run()
