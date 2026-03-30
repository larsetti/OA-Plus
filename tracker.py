#!/usr/bin/env python3
"""
Berlin Ordnungsamt Mull-Tracker
================================
Taglich ausfuhren via GitHub Actions (06:00 UTC).
- Ladt alle Meldungen von der API
- Speichert neue Meldungen in der DB
- Ruft fuer neue Mull-Meldungen den Detail-Endpunkt ab (Koordinaten!)
- Berechnet Hotspot-Scores
"""

import sqlite3
import json
import requests
import hashlib
import logging
import time
from datetime import datetime
from pathlib import Path

# Konfiguration
DB_PATH = Path(__file__).parent / "ordnungsamt.db"
API_URL = "https://ordnungsamt.berlin.de/frontend.webservice.opendata/api/meldungen"
API_DETAIL = "https://ordnungsamt.berlin.de/frontend.webservice.opendata/api/meldungen/{}"

# Alle Mull-Kategorien aus der Ordnungsamt-App
MUELL_KEYWORDS = [
    "abfall", "autowrack", "bauabfalle", "bauschutt", "bioabfalle",
    "elektroschrott", "mullablagerung", "mull", "papierkorbe",
    "schrottfahrrader", "sperrm", "tierkadaver", "tote tiere",
    "unrat", "weihnachtsbaume", "gewerbebetrieb", "flaschen",
    "abgelagert", "fasser", "grunanlage", "kfz-teile", "betriebsstoffe",
    "kanister", "entsorgung", "ablagerung", "deponie", "sondermu",
    "grunschnitt", "schrottauto", "sperrm\u00fcll", "m\u00fcll",
    "abf\u00e4lle", "gr\u00fcnschnitt", "gr\u00fcnanlage",
]

GEO_RADIUS = 0.0015
DISPOSAL_DAYS = 14

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(Path(__file__).parent / "tracker.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meldungen (
            id           TEXT PRIMARY KEY,
            fetched_at   TEXT NOT NULL,
            datum        TEXT,
            kategorie    TEXT,
            betreff      TEXT,
            bezirk       TEXT,
            lat          REAL,
            lon          REAL,
            status       TEXT,
            is_muell     INTEGER DEFAULT 0,
            strasse      TEXT DEFAULT '',
            hausNummer   TEXT DEFAULT '',
            plz          TEXT DEFAULT '',
            enriched     INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_latlon  ON meldungen(lat, lon);
        CREATE INDEX IF NOT EXISTS idx_datum   ON meldungen(datum);
        CREATE INDEX IF NOT EXISTS idx_bezirk  ON meldungen(bezirk);
        CREATE INDEX IF NOT EXISTS idx_ismuell ON meldungen(is_muell);

        CREATE TABLE IF NOT EXISTS hotspots (
            cluster_id       TEXT PRIMARY KEY,
            lat_center       REAL,
            lon_center       REAL,
            bezirk           TEXT,
            meldungen_count  INTEGER DEFAULT 0,
            recurrence_count INTEGER DEFAULT 0,
            last_seen        TEXT,
            first_seen       TEXT,
            score            REAL DEFAULT 0.0,
            score_label      TEXT DEFAULT 'niedrig'
        );

        CREATE TABLE IF NOT EXISTS fetch_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at  TEXT,
            count_total INTEGER,
            count_new   INTEGER,
            count_muell INTEGER
        );
    """)
    conn.commit()

    # Spalten nachrüsten falls DB älter
    for col, typedef in [
        ("strasse",    "TEXT DEFAULT ''"),
        ("hausNummer", "TEXT DEFAULT ''"),
        ("plz",        "TEXT DEFAULT ''"),
        ("enriched",   "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE meldungen ADD COLUMN {col} {typedef}")
            conn.commit()
        except sqlite3.OperationalError:
            pass

    log.info("Datenbank initialisiert: %s", DB_PATH)


def fetch_all_meldungen():
    """Ladt alle Meldungen von der Ubersichts-API (~26MB)."""
    local = Path(__file__).parent / "meldungen.json"
    if local.exists():
        log.info("Lese lokale Datei: %s (%.1f MB)", local, local.stat().st_size / 1024 / 1024)
        data = json.loads(local.read_text(encoding="utf-8"))
        meldungen = data if isinstance(data, list) else data.get("index", data.get("meldungen", []))
        if meldungen:
            log.info("Erfolg: %d Meldungen aus lokaler Datei", len(meldungen))
            return meldungen

    log.info("Starte Download (~26MB, bitte warten)...")
    for attempt in range(3):
        try:
            resp = requests.get(API_URL, timeout=(30, 600), headers=HEADERS, stream=True)
            if resp.status_code == 200:
                chunks = []
                total = 0
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        chunks.append(chunk)
                        total += len(chunk)
                        if total % (1024 * 1024) < 65536:
                            log.info("  %.1f MB geladen...", total / 1024 / 1024)
                raw = b"".join(chunks)
                data = json.loads(raw.decode("utf-8"))
                meldungen = data if isinstance(data, list) else data.get("index", data.get("meldungen", []))
                if meldungen:
                    log.info("Erfolg: %d Meldungen erhalten", len(meldungen))
                    return meldungen
            else:
                log.warning("HTTP %d", resp.status_code)
        except Exception as e:
            log.warning("Fehler (Versuch %d/3): %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(30)
    return []


def fetch_detail(session, meldung_id):
    """Detail-Endpunkt fuer eine Meldung — liefert Koordinaten und Adresse."""
    url = API_DETAIL.format(meldung_id)
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
                log.warning("HTTP 503 - warte %ds", wait)
                time.sleep(wait)
            else:
                log.warning("HTTP %d fuer ID %s", resp.status_code, meldung_id)
                return None
        except requests.exceptions.Timeout:
            log.warning("Timeout fuer ID %s", meldung_id)
            time.sleep(30)
        except Exception as e:
            log.warning("Fehler fuer ID %s: %s", meldung_id, e)
            return None
    return None


def is_muell(m):
    text = " ".join([
        str(m.get("betreff", "")),
        str(m.get("kategorie", "")),
        str(m.get("sachverhalt", "")),
    ]).lower()
    return any(kw in text for kw in MUELL_KEYWORDS)


def make_id(m):
    raw_id = m.get("id") or m.get("meldungsId")
    if raw_id:
        return str(raw_id)
    return "hash_" + hashlib.md5(json.dumps(m, sort_keys=True).encode()).hexdigest()[:16]


def cluster_id(lat, lon):
    cell_lat = round(lat / GEO_RADIUS) * GEO_RADIUS
    cell_lon = round(lon / GEO_RADIUS) * GEO_RADIUS
    return f"{cell_lat:.5f}_{cell_lon:.5f}"


def compute_score(count, recurrence, days_since_first):
    base = count + recurrence * 3
    time_factor = max(0.5, 1 - (days_since_first / 365) * 0.3)
    score = round(base * time_factor, 2)
    if score < 4:    label = "niedrig"
    elif score < 8:  label = "mittel"
    elif score < 13: label = "hoch"
    else:            label = "kritisch"
    return score, label


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    now = datetime.utcnow().isoformat()
    meldungen = fetch_all_meldungen()
    log.info("%d Meldungen von API erhalten", len(meldungen))

    count_new = 0
    count_muell = 0
    new_ids = []

    for m in meldungen:
        mid = make_id(m)

        if conn.execute("SELECT id FROM meldungen WHERE id=?", (mid,)).fetchone():
            continue

        muell = is_muell(m)
        datum = m.get("erstellungsDatum") or m.get("datum") or now[:10]

        conn.execute("""
            INSERT INTO meldungen
                (id, fetched_at, datum, kategorie, betreff, bezirk, status, is_muell, enriched)
            VALUES (?,?,?,?,?,?,?,?,0)
        """, (
            mid, now, datum,
            m.get("kategorie", ""),
            m.get("betreff", ""),
            m.get("bezirk", ""),
            m.get("status", ""),
            1 if muell else 0,
        ))
        count_new += 1
        if muell:
            count_muell += 1
            new_ids.append(mid)

    conn.commit()
    log.info("Neu gespeichert: %d Meldungen (%d Mull)", count_new, count_muell)

    # Detail-Abruf fuer neue Mull-Meldungen (Koordinaten!)
    if new_ids:
        log.info("Hole Koordinaten fuer %d neue Mull-Meldungen...", len(new_ids))
        session = requests.Session()
        enriched = 0
        for mid in new_ids:
            detail = fetch_detail(session, mid)
            if detail:
                lat = detail.get("lat")
                lng = detail.get("lng") or detail.get("lon")
                strasse = detail.get("strasse", "")
                haus = detail.get("hausNummer", "")
                plz = detail.get("plz", "")
                conn.execute("""
                    UPDATE meldungen
                    SET lat=?, lon=?, strasse=?, hausNummer=?, plz=?, enriched=1
                    WHERE id=?
                """, (lat, lng, strasse, haus, plz, mid))
                enriched += 1
            time.sleep(0.5)
        conn.commit()
        log.info("Koordinaten geholt fuer %d Meldungen", enriched)

    # Hotspot-Berechnung
    muell_rows = conn.execute("""
        SELECT id, datum, lat, lon, bezirk
        FROM meldungen
        WHERE is_muell=1 AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY datum ASC
    """).fetchall()

    clusters = {}
    for row in muell_rows:
        cid = cluster_id(row["lat"], row["lon"])
        if cid not in clusters:
            clusters[cid] = {"lats": [], "lons": [], "dates": [], "bezirk": row["bezirk"], "recurrence": 0}
        clusters[cid]["lats"].append(row["lat"])
        clusters[cid]["lons"].append(row["lon"])
        clusters[cid]["dates"].append(row["datum"] or "")

    for cid, c in clusters.items():
        dates_sorted = sorted(d for d in c["dates"] if d)
        for i in range(1, len(dates_sorted)):
            try:
                d1 = datetime.fromisoformat(dates_sorted[i-1][:10])
                d2 = datetime.fromisoformat(dates_sorted[i][:10])
                if 0 < (d2 - d1).days <= (DISPOSAL_DAYS + 7):
                    c["recurrence"] += 1
            except Exception:
                pass

    for cid, c in clusters.items():
        lat_c = sum(c["lats"]) / len(c["lats"])
        lon_c = sum(c["lons"]) / len(c["lons"])
        dates_sorted = sorted(d for d in c["dates"] if d)
        first = dates_sorted[0] if dates_sorted else ""
        last = dates_sorted[-1] if dates_sorted else ""
        try:
            days_age = (datetime.utcnow() - datetime.fromisoformat(first[:10])).days
        except Exception:
            days_age = 0

        score, label = compute_score(len(c["dates"]), c["recurrence"], days_age)

        conn.execute("""
            INSERT INTO hotspots
                (cluster_id, lat_center, lon_center, bezirk, meldungen_count,
                 recurrence_count, last_seen, first_seen, score, score_label)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cluster_id) DO UPDATE SET
                meldungen_count  = excluded.meldungen_count,
                recurrence_count = excluded.recurrence_count,
                last_seen        = excluded.last_seen,
                score            = excluded.score,
                score_label      = excluded.score_label
        """, (cid, lat_c, lon_c, c["bezirk"], len(c["dates"]),
              c["recurrence"], last, first, score, label))

    conn.commit()

    conn.execute("""
        INSERT INTO fetch_log (fetched_at, count_total, count_new, count_muell)
        VALUES (?,?,?,?)
    """, (now, len(meldungen), count_new, count_muell))
    conn.commit()

    log.info("Fertig: %d neu, %d Mull, %d Hotspots", count_new, count_muell, len(clusters))
    conn.close()


if __name__ == "__main__":
    run()
