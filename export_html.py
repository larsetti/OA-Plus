#!/usr/bin/env python3
"""
export_html.py
==============
Liest die aktuelle ordnungsamt.db und generiert eine neue index.html.
Wird täglich nach tracker.py vom GitHub Actions Workflow aufgerufen.
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime

DB_PATH  = Path(__file__).parent / "ordnungsamt.db"
HTML_TEMPLATE = Path(__file__).parent / "template.html"
OUT_PATH = Path(__file__).parent / "index.html"


def load_data() -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    hotspots = [dict(r) for r in conn.execute(
        "SELECT * FROM hotspots ORDER BY score DESC"
    ).fetchall()]

    meldungen = [dict(r) for r in conn.execute("""
        SELECT id, datum, kategorie, betreff, bezirk, lat, lon, is_muell, status
        FROM meldungen WHERE lat IS NOT NULL ORDER BY datum DESC LIMIT 500
    """).fetchall()]

    bezirk_stats = [dict(r) for r in conn.execute("""
        SELECT bezirk,
               COUNT(*) as total_hotspots,
               SUM(meldungen_count) as total_meldungen,
               SUM(recurrence_count) as total_recurrence,
               ROUND(MAX(score), 1) as max_score,
               SUM(CASE WHEN score_label='kritisch' THEN 1 ELSE 0 END) as krit,
               SUM(CASE WHEN score_label='hoch'     THEN 1 ELSE 0 END) as hoch
        FROM hotspots GROUP BY bezirk ORDER BY max_score DESC
    """).fetchall()]

    fetch_log = [dict(r) for r in conn.execute(
        "SELECT * FROM fetch_log ORDER BY id DESC LIMIT 1"
    ).fetchall()]

    conn.close()
    return {
        "hotspots":    hotspots,
        "meldungen":   meldungen,
        "bezirk_stats": bezirk_stats,
        "last_update": fetch_log[0]["fetched_at"][:10] if fetch_log else datetime.now().strftime("%Y-%m-%d"),
        "generated_at": datetime.now().isoformat()
    }


def build_html(data: dict) -> str:
    compact = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

    # Lese Template, ersetze Daten-Platzhalter
    template = HTML_TEMPLATE.read_text(encoding="utf-8")
    html = template.replace("__APP_DATA_PLACEHOLDER__", compact)
    html = html.replace("__LAST_UPDATE__", data["last_update"])
    return html


def main():
    print(f"Lade Daten aus {DB_PATH}...")
    data = load_data()
    print(f"  {len(data['hotspots'])} Hotspots, {len(data['meldungen'])} Meldungen")

    print("Generiere index.html...")
    html = build_html(data)
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"  Gespeichert: {OUT_PATH} ({len(html):,} Zeichen)")


if __name__ == "__main__":
    main()
