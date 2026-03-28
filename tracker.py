#!/usr/bin/env python3
"""
Berlin Ordnungsamt Müll-Tracker
================================
Täglich ausführen via Cron: 0 6 * * * /usr/bin/python3 /pfad/zu/tracker.py

Legt alle Meldungen in einer SQLite-Datenbank ab und berechnet
Hotspot-Scores für wiederkehrende Müllmeldungen.
"""

import sqlite3
import json
import requests
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ── Konfiguration ─────────────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "ordnungsamt.db"
API_URL  = "https://ordnungsamt.berlin.de/frontend.webservice.opendata/api/meldungen"

# Kategorien, die als "Müll" gewertet werden (Schlüsselwörter im Betreff/Kategorie)
MUELL_KEYWORDS = [
    "müll", "abfall", "sperrmüll", "entsorgung", "ablagerung",
    "illegal", "deponie", "schutt", "bauschutt", "schrottauto",
    "sondermüll", "elektroschrott", "grünschnitt"
]

# Radius in Grad (~150m) für Geo-Clustering
GEO_RADIUS = 0.0015

# Tage bis zur "regulären" Entsorgung (Berliner Realität)
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


# ── Datenbank-Setup ───────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meldungen (
            id          TEXT PRIMARY KEY,
            fetched_at  TEXT NOT NULL,
            datum       TEXT,
            kategorie   TEXT,
            betreff     TEXT,
            bezirk      TEXT,
            lat         REAL,
            lon         REAL,
            status      TEXT,
            is_muell    INTEGER DEFAULT 0,
            raw_json    TEXT,
            strasse     TEXT DEFAULT '',
            plz         TEXT DEFAULT ''
        );
        -- Spalten nachrüsten falls DB bereits existiert
        CREATE INDEX IF NOT EXISTS idx_latlon  ON meldungen(lat, lon);

        CREATE TABLE IF NOT EXISTS hotspots (
            cluster_id      TEXT PRIMARY KEY,
            lat_center      REAL,
            lon_center      REAL,
            bezirk          TEXT,
            meldungen_count INTEGER DEFAULT 0,
            recurrence_count INTEGER DEFAULT 0,
            last_seen       TEXT,
            first_seen      TEXT,
            score           REAL DEFAULT 0.0,
            score_label     TEXT DEFAULT 'niedrig'
        );

        CREATE INDEX IF NOT EXISTS idx_latlon  ON meldungen(lat, lon);
        CREATE INDEX IF NOT EXISTS idx_datum   ON meldungen(datum);
        CREATE INDEX IF NOT EXISTS idx_bezirk  ON meldungen(bezirk);
        CREATE INDEX IF NOT EXISTS idx_ismuell ON meldungen(is_muell);

        CREATE TABLE IF NOT EXISTS fetch_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at  TEXT,
            count_total INTEGER,
            count_new   INTEGER,
            count_muell INTEGER
        );
    """)
    conn.commit()
    log.info("Datenbank initialisiert: %s", DB_PATH)


# ── API-Abruf ─────────────────────────────────────────────────────────────────
def fetch_meldungen() -> list[dict]:
    log.info("Abrufe API: %s", API_URL)
    import time
    for attempt in range(3):
        try:
            resp = requests.get(API_URL, timeout=60, headers={"Accept": "application/json"})
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return data.get("meldungen", data.get("data", []))
        except Exception as e:
            log.warning("API-Versuch %d fehlgeschlagen: %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(10)
    log.warning("API nicht erreichbar nach 3 Versuchen - ueberspringe Fetch")
    return []


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────
def is_muell(m: dict) -> bool:
    text = " ".join([
        str(m.get("kategorie", "")),
        str(m.get("betreff",   "")),
        str(m.get("bereich",   "")),
        str(m.get("beschreibung", ""))
    ]).lower()
    return any(kw in text for kw in MUELL_KEYWORDS)


def extract_coords(m: dict) -> tuple[float | None, float | None]:
    """Koordinaten aus verschiedenen möglichen API-Strukturen extrahieren."""
    # Variante 1: flach
    lat = m.get("lat") or m.get("latitude") or m.get("breitengrad")
    lon = m.get("lon") or m.get("lng") or m.get("longitude") or m.get("laengengrad")
    # Variante 2: verschachtelt
    if not lat and "position" in m:
        lat = m["position"].get("lat") or m["position"].get("latitude")
        lon = m["position"].get("lon") or m["position"].get("lng")
    if not lat and "koordinaten" in m:
        lat = m["koordinaten"].get("lat")
        lon = m["koordinaten"].get("lon")
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None, None


def make_id(m: dict) -> str:
    """Stabiles ID aus API-Feldern oder Hash des Inhalts."""
    raw_id = m.get("id") or m.get("meldungsId") or m.get("meldung_id")
    if raw_id:
        return str(raw_id)
    digest = hashlib.md5(json.dumps(m, sort_keys=True).encode()).hexdigest()
    return f"hash_{digest[:16]}"


def cluster_id(lat: float, lon: float) -> str:
    """Geo-Zelle als Cluster-Schlüssel (~150m Raster)."""
    cell_lat = round(lat / GEO_RADIUS) * GEO_RADIUS
    cell_lon = round(lon / GEO_RADIUS) * GEO_RADIUS
    return f"{cell_lat:.5f}_{cell_lon:.5f}"


# ── Hotspot-Score-Berechnung ──────────────────────────────────────────────────
def compute_score(count: int, recurrence: int, days_since_first: int) -> tuple[float, str]:
    """
    Score-Logik:
    - Jede Meldung: +1 Basispunkt
    - Wiederkehrende Meldung (< DISPOSAL_DAYS+7 nach letzter): +3 Punkte (Indikator für chronischen Hotspot)
    - Zeitfaktor: schnelle Wiederkehr erhöht Score
    - Alter dämpft Score leicht (alte inaktive Spots fallen ab)

    Labels: niedrig (0-3) | mittel (4-7) | hoch (8-12) | kritisch (13+)
    """
    base = count + recurrence * 3
    time_factor = max(0.5, 1 - (days_since_first / 365) * 0.3)
    score = round(base * time_factor, 2)

    if score < 4:
        label = "niedrig"
    elif score < 8:
        label = "mittel"
    elif score < 13:
        label = "hoch"
    else:
        label = "kritisch"
    return score, label


# ── Hauptlogik ────────────────────────────────────────────────────────────────
def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    now = datetime.utcnow().isoformat()
    meldungen = fetch_meldungen()
    log.info("%d Meldungen von API erhalten", len(meldungen))

    count_new = 0
    count_muell = 0

    for m in meldungen:
        mid  = make_id(m)
        lat, lon = extract_coords(m)
        muell = is_muell(m)
        datum = m.get("datum") or m.get("erstelltAm") or m.get("created_at") or now[:10]

        existing = conn.execute("SELECT id FROM meldungen WHERE id=?", (mid,)).fetchone()
        if existing:
            continue  # bereits in DB

        # Adresse aus verschiedenen möglichen API-Feldern extrahieren
        strasse = (m.get("strasse") or m.get("street") or m.get("strasseOrt") or
                   m.get("adresse") or m.get("address") or m.get("ort") or "")
        plz_val = m.get("plz") or m.get("postleitzahl") or m.get("zip") or ""

        conn.execute("""
            INSERT INTO meldungen
                (id, fetched_at, datum, kategorie, betreff, bezirk, lat, lon, status, is_muell, raw_json, strasse, plz)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            mid, now, datum,
            m.get("kategorie") or m.get("category", ""),
            m.get("betreff")   or m.get("subject",  ""),
            m.get("bezirk")    or m.get("district",  ""),
            lat, lon,
            m.get("status", ""),
            1 if muell else 0,
            json.dumps(m, ensure_ascii=False),
            strasse, str(plz_val)
        ))
        count_new += 1
        if muell:
            count_muell += 1

    conn.commit()

    # ── Hotspot-Berechnung (nur Müll-Meldungen mit Koordinaten) ──────────────
    muell_rows = conn.execute("""
        SELECT id, datum, lat, lon, bezirk
        FROM meldungen
        WHERE is_muell=1 AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY datum ASC
    """).fetchall()

    clusters: dict[str, dict] = {}
    for row in muell_rows:
        cid = cluster_id(row["lat"], row["lon"])
        if cid not in clusters:
            clusters[cid] = {
                "lats": [], "lons": [], "dates": [],
                "bezirk": row["bezirk"], "recurrence": 0
            }
        c = clusters[cid]
        c["lats"].append(row["lat"])
        c["lons"].append(row["lon"])
        c["dates"].append(row["datum"])

    # Wiederkehr-Erkennung: Meldung < DISPOSAL_DAYS+7 nach vorheriger → Hotspot-Indikator
    for cid, c in clusters.items():
        dates_sorted = sorted(c["dates"])
        for i in range(1, len(dates_sorted)):
            try:
                d1 = datetime.fromisoformat(dates_sorted[i-1][:10])
                d2 = datetime.fromisoformat(dates_sorted[i][:10])
                gap = (d2 - d1).days
                if 0 < gap <= (DISPOSAL_DAYS + 7):
                    c["recurrence"] += 1
            except Exception:
                pass

    # Hotspots in DB schreiben
    for cid, c in clusters.items():
        lat_c = sum(c["lats"]) / len(c["lats"])
        lon_c = sum(c["lons"]) / len(c["lons"])
        dates_sorted = sorted(c["dates"])
        first = dates_sorted[0]
        last  = dates_sorted[-1]
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

    log.info("Fertig: %d neu, %d Müll-Meldungen, %d Hotspots berechnet",
             count_new, count_muell, len(clusters))
    conn.close()


if __name__ == "__main__":
    run()
