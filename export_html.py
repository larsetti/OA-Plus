#!/usr/bin/env python3
import sqlite3, json
from datetime import datetime
from collections import Counter
from pathlib import Path

DB_PATH = Path(__file__).parent / "ordnungsamt.db"
TEMPLATE = Path(__file__).parent / "template.html"
OUT_PATH = Path(__file__).parent / "index.html"

GEO_RADIUS = 0.0015
WEEKDAYS_SHORT = ['Mo','Di','Mi','Do','Fr','Sa','So']

# Alle echten Kategorien aus der Ordnungsamt-App — keine Paragrafen-Hinweise
KATEGORIE_GRUPPEN = {
    'sperrmüll':      {'keywords':['sperrmüll','sperr','sofa','matratze','kühlschrank','möbel'],'label':'🛋 Sperrmüll','color':'#996600'},
    'bauschutt':      {'keywords':['bauschutt','bauabfälle','schutt','baumaterial'],'label':'🏗 Bauschutt','color':'#8B4513'},
    'elektroschrott': {'keywords':['elektroschrott','elektro','e-schrott'],'label':'⚡ Elektroschrott','color':'#0066aa'},
    'gartenabfall':   {'keywords':['bioabfälle','gartenabfall','grünschnitt','grünanlage','weihnachtsbäume'],'label':'🌿 Grünabfall','color':'#2d7d2d'},
    'schrottfahrzeug':{'keywords':['autowrack','schrottfahrräder','kfz-teile','betriebsstoffe','schrottauto','fahrzeug'],'label':'🚗 Schrott-KFZ','color':'#555555'},
    'gefahrstoffe':   {'keywords':['kanister','fässer','flaschen','unbekannte stoffe','gefahrstoff','sondermüll'],'label':'☢️ Gefahrstoffe','color':'#cc0000'},
    'tierisch':       {'keywords':['tierkadaver','tote tiere','hundekot'],'label':'🐾 Tierisch','color':'#7b3fa0'},
    'unrat':          {'keywords':['unrat','werbezettel','papierkörbe','müllablagerung','sonstiges','abfall'],'label':'🚮 Unrat/Sonstiges','color':'#888888'},
}

def parse_datum(s):
    """Parst '18.01.2017 - 15:01:09' und andere Formate."""
    if not s:
        return None
    for fmt in ('%d.%m.%Y - %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(s[:len(fmt)+2].strip(), fmt)
        except:
            pass
    return None

def kategorisiere(text):
    t = (text or '').lower()
    for key, grp in KATEGORIE_GRUPPEN.items():
        if any(kw in t for kw in grp['keywords']):
            return key
    return 'unrat'

def load_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Alle Müll-Meldungen mit Koordinaten
    muell = conn.execute("""
        SELECT datum, erstellungsDatum, lat, lon, kategorie, betreff, strasse, hausNummer, plz
        FROM meldungen
        WHERE is_muell=1 AND lat IS NOT NULL
    """).fetchall() if _col_exists(conn, 'erstellungsDatum') else conn.execute("""
        SELECT datum, lat, lon, kategorie, betreff, strasse, plz
        FROM meldungen
        WHERE is_muell=1 AND lat IS NOT NULL
    """).fetchall()

    cluster_m = {}
    for row in muell:
        cid = f"{round(row['lat']/GEO_RADIUS)*GEO_RADIUS:.5f}_{round(row['lon']/GEO_RADIUS)*GEO_RADIUS:.5f}"
        if cid not in cluster_m:
            cluster_m[cid] = []
        # Datum aus erstellungsDatum oder datum
        datum_str = None
        try:
            datum_str = row['erstellungsDatum']
        except:
            pass
        if not datum_str:
            datum_str = row['datum'] or ''
        cluster_m[cid].append({
            'datum': datum_str,
            'kategorie': row['kategorie'] or '',
            'betreff': row['betreff'] or '',
            'strasse': row['strasse'] or '',
        })

    hotspots = [dict(r) for r in conn.execute("SELECT * FROM hotspots ORDER BY score DESC").fetchall()]

    for h in hotspots:
        cid = h['cluster_id']
        meldungen = cluster_m.get(cid, [])
        total = len(meldungen) or 1

        # Kategorie-Mix
        grp_list = [kategorisiere(m['kategorie'] + ' ' + m['betreff']) for m in meldungen]
        grp_count = Counter(grp_list)
        h['kategorie_mix'] = [
            {
                'key': k,
                'label': KATEGORIE_GRUPPEN[k]['label'],
                'color': KATEGORIE_GRUPPEN[k]['color'],
                'count': c,
                'pct': round(c / total * 100)
            }
            for k, c in grp_count.most_common()
            if k in KATEGORIE_GRUPPEN
        ]
        h['top_kategorie'] = grp_count.most_common(1)[0][0] if grp_count else None
        h['top_kategorie_pct'] = round(grp_count.most_common(1)[0][1] / total * 100) if grp_count else 0

        # Adresse aus Meldungen holen (erste mit Straße)
        if not h.get('strasse'):
            for m in meldungen:
                if m.get('strasse'):
                    h['strasse'] = m['strasse']
                    break

        # Wochentag-Analyse
        weekdays = []
        parsed_dates = []
        for m in meldungen:
            d = parse_datum(m['datum'])
            if d:
                weekdays.append(d.weekday())
                parsed_dates.append(d)

        h['weekday_dist'] = {d: 0 for d in WEEKDAYS_SHORT}
        h['pattern'] = 'normal'
        h['pattern_label'] = ''
        h['auffaelligkeiten'] = []

        if weekdays:
            cnt = Counter(weekdays)
            for i, d in enumerate(WEEKDAYS_SHORT):
                h['weekday_dist'][d] = cnt.get(i, 0)
            tw = len(weekdays)
            mon_r = cnt.get(0, 0) / tw
            wknd_r = (cnt.get(5, 0) + cnt.get(6, 0)) / tw
            if mon_r >= 0.35 and tw >= 2:
                h['pattern'] = 'montag'
                h['pattern_label'] = f"{int(mon_r*100)}% Montags"
                h['auffaelligkeiten'].append(f"Häufung am Montag ({int(mon_r*100)}%) — Wochenend-Ablagerungen")
            elif wknd_r >= 0.35 and tw >= 2:
                h['pattern'] = 'wochenende'
                h['pattern_label'] = f"{int(wknd_r*100)}% Wochenende"
                h['auffaelligkeiten'].append(f"Häufung am Wochenende ({int(wknd_r*100)}%)")

        if h['recurrence_count'] >= 3:
            h['auffaelligkeiten'].append(f"Chronischer Ablagerungsort: {h['recurrence_count']}× Wiederkehr")

        # Saisonale Analyse
        if parsed_dates:
            seasons = []
            month_list = []
            monthend_count = 0
            for d in parsed_dates:
                month_list.append(d.month)
                if d.day >= 25:
                    monthend_count += 1
                if d.month in [3, 4, 5]: seasons.append('frühling')
                elif d.month in [6, 7, 8]: seasons.append('sommer')
                elif d.month in [9, 10, 11]: seasons.append('herbst')
                else: seasons.append('winter')

            if seasons:
                season_cnt = Counter(seasons)
                top_season, top_season_n = season_cnt.most_common(1)[0]
                season_ratio = top_season_n / len(seasons)
                SEASON_LABELS = {
                    'frühling': '🌸 Frühlings-Häufung',
                    'sommer':   '☀️ Sommer-Häufung',
                    'herbst':   '🍂 Herbst-Häufung',
                    'winter':   '❄️ Winter-Häufung',
                }
                if season_ratio >= 0.6 and len(seasons) >= 3:
                    h['auffaelligkeiten'].append(f"{SEASON_LABELS[top_season]} ({int(season_ratio*100)}%)")

            if len(meldungen) >= 3:
                monthend_ratio = monthend_count / len(meldungen)
                if monthend_ratio >= 0.5:
                    h['auffaelligkeiten'].append(f"📅 {int(monthend_ratio*100)}% der Meldungen zum Monatsende — Hinweis auf Wohnungswechsel")

        # Kategorienvielfalt
        if total >= 4:
            unique_kats = len(set(g for g in grp_list if g))
            if unique_kats >= 3:
                h['auffaelligkeiten'].append(f"⚠️ {unique_kats} verschiedene Abfallarten — Standort wird von vielen Verursachern genutzt")

    bezirk_stats = [dict(r) for r in conn.execute("""
        SELECT bezirk, COUNT(*) as total_hotspots, SUM(meldungen_count) as total_meldungen,
               SUM(recurrence_count) as total_recurrence, ROUND(MAX(score),1) as max_score,
               SUM(CASE WHEN score_label='kritisch' THEN 1 ELSE 0 END) as krit,
               SUM(CASE WHEN score_label='hoch' THEN 1 ELSE 0 END) as hoch
        FROM hotspots GROUP BY bezirk ORDER BY max_score DESC
    """).fetchall()]
    conn.close()

    return {
        "hotspots": hotspots,
        "bezirk_stats": bezirk_stats,
        "bezirke": sorted(set(h['bezirk'] for h in hotspots if h['bezirk'])),
        "kat_keys": {k: {'label': v['label'], 'color': v['color']} for k, v in KATEGORIE_GRUPPEN.items()},
        "last_update": datetime.now().strftime("%Y-%m-%d"),
    }

def _col_exists(conn, col):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(meldungen)").fetchall()]
    return col in cols

def main():
    print(f"Lade Daten aus {DB_PATH}...")
    data = load_data()
    print(f"  {len(data['hotspots'])} Hotspots")
    compact = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    tmpl = TEMPLATE.read_text(encoding='utf-8')
    html = tmpl.replace('__APP_DATA_PLACEHOLDER__', compact).replace('__LAST_UPDATE__', data['last_update'])
    OUT_PATH.write_text(html, encoding='utf-8')
    print(f"  Gespeichert: {OUT_PATH}")

if __name__ == "__main__":
    main()
