#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3, json
from datetime import datetime
from collections import Counter
from pathlib import Path

DB_PATH = Path(__file__).parent / "ordnungsamt.db"
TEMPLATE = Path(__file__).parent / "template.html"
OUT_PATH = Path(__file__).parent / "index.html"

GEO_RADIUS = 0.0015
WEEKDAYS_SHORT = ['Mo','Di','Mi','Do','Fr','Sa','So']
MONAT_NAMEN = ['Januar','Februar','März','April','Mai','Juni',
               'Juli','August','September','Oktober','November','Dezember']

KATEGORIE_GRUPPEN = {
    'sperrmüll':      {'keywords':['sperrmüll','sperr','sofa','matratze','kühlschrank','möbel'],
                       'label':'🛋 Sperrmüll','color':'#996600'},
    'bauschutt':      {'keywords':['bauschutt','bauabfälle','schutt','baumaterial'],
                       'label':'🏗 Bauschutt','color':'#8B4513'},
    'elektroschrott': {'keywords':['elektroschrott','elektro','e-schrott'],
                       'label':'⚡ E-Schrott','color':'#0066aa'},
    'gartenabfall':   {'keywords':['bioabfälle','gartenabfall','grünschnitt','grünanlage','weihnachtsbäume'],
                       'label':'🌿 Grünabfall','color':'#2d7d2d'},
    'schrottfahrzeug':{'keywords':['autowrack','schrottfahrräder','kfz-teile','betriebsstoffe','schrottauto'],
                       'label':'🚗 Schrott-KFZ','color':'#555555'},
    'gefahrstoffe':   {'keywords':['kanister','fässer','flaschen','unbekannte stoffe','sondermüll'],
                       'label':'☢️ Gefahrstoffe','color':'#cc0000'},
    'tierisch':       {'keywords':['tierkadaver','tote tiere','hundekot'],
                       'label':'🐾 Tierisch','color':'#7b3fa0'},
    'illegal':        {'keywords':['illegal','ablagerung','müllablagerung','unrat','müll','abfall'],
                       'label':'🚮 Illeg. Ablag.','color':'#cc0000'},
}

def kategorisiere(text):
    t = (text or '').lower()
    for key, grp in KATEGORIE_GRUPPEN.items():
        if any(kw in t for kw in grp['keywords']):
            return key
    return 'illegal'

def parse_datum(s):
    if not s: return None
    for fmt in ('%d.%m.%Y - %H:%M:%S', '%d.%m.%Y', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(s[:len(fmt)+2].strip(), fmt)
        except: pass
    return None

def get_saison(month):
    if month in [3,4,5]: return 'frühling'
    if month in [6,7,8]: return 'sommer'
    if month in [9,10,11]: return 'herbst'
    return 'winter'

def load_data():
    today = datetime.now()
    today_wd = today.weekday()
    today_month = today.month
    today_kw = today.isocalendar()[1]
    today_saison = get_saison(today_month)
    # Nächste 4 Wochen Monate
    naechste_monate = set()
    for offset in range(5):
        m = ((today_month - 1 + offset) % 12) + 1
        naechste_monate.add(m)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    muell = conn.execute("""
        SELECT datum, lat, lon, kategorie, betreff, strasse, plz
        FROM meldungen WHERE is_muell=1 AND lat IS NOT NULL
    """).fetchall()

    cluster_m = {}
    for row in muell:
        cid = f"{round(row['lat']/GEO_RADIUS)*GEO_RADIUS:.5f}_{round(row['lon']/GEO_RADIUS)*GEO_RADIUS:.5f}"
        if cid not in cluster_m: cluster_m[cid] = []
        cluster_m[cid].append({
            'datum': row['datum'] or '',
            'kategorie': row['kategorie'] or '',
            'betreff': row['betreff'] or '',
            'strasse': row['strasse'] or '',
            'plz': row['plz'] or '',
        })

    hotspots = [dict(r) for r in conn.execute(
        "SELECT * FROM hotspots ORDER BY score DESC"
    ).fetchall()]

    for h in hotspots:
        cid = h['cluster_id']
        meldungen = cluster_m.get(cid, [])
        total = len(meldungen) or 1

        # Adresse
        if not h.get('strasse'):
            for m in meldungen:
                if m.get('strasse'):
                    h['strasse'] = m['strasse']
                    break
        if not h.get('plz'):
            for m in meldungen:
                if m.get('plz'):
                    h['plz'] = m['plz']
                    break

        # Kategorie-Mix
        grp_list = [kategorisiere(m['kategorie'] + ' ' + m['betreff']) for m in meldungen]
        grp_count = Counter(grp_list)
        h['kategorie_mix'] = [
            {'key': k, 'label': KATEGORIE_GRUPPEN[k]['label'],
             'color': KATEGORIE_GRUPPEN[k]['color'], 'count': c, 'pct': round(c/total*100)}
            for k, c in grp_count.most_common() if k in KATEGORIE_GRUPPEN
        ]
        h['top_kategorie'] = grp_count.most_common(1)[0][0] if grp_count else None
        h['top_kategorie_pct'] = round(grp_count.most_common(1)[0][1]/total*100) if grp_count else 0

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
                h['auffaelligkeiten'].append(f"Häufung am Montag ({int(mon_r*100)}%) — Ablagerung meist am Wochenende")
            elif wknd_r >= 0.35 and tw >= 2:
                h['pattern'] = 'wochenende'
                h['pattern_label'] = f"{int(wknd_r*100)}% Wochenende"
                h['auffaelligkeiten'].append(f"Häufung am Wochenende ({int(wknd_r*100)}%)")

        if h['recurrence_count'] >= 3:
            h['auffaelligkeiten'].append(f"Chronischer Ablagerungsort: {h['recurrence_count']}× Wiederkehr")

        # Saisonale Analyse — NUR aktuelle/kommende Saison anzeigen
        if parsed_dates:
            seasons = []
            month_list = []
            monthend_count = 0
            for d in parsed_dates:
                month_list.append(d.month)
                if d.day >= 25: monthend_count += 1
                seasons.append(get_saison(d.month))

            if seasons:
                sc = Counter(seasons)
                ts, tsn = sc.most_common(1)[0]
                sr = tsn / len(seasons)
                # NUR anzeigen wenn aktuelle oder nächste Saison betroffen
                if sr >= 0.5 and len(seasons) >= 3:
                    naechste_saison = get_saison(((today_month) % 12) + 1)
                    if ts == today_saison or ts == naechste_saison:
                        SL = {'frühling':'🌸 Frühlings-Hotspot','sommer':'☀️ Sommer-Hotspot',
                              'herbst':'🍂 Herbst-Hotspot','winter':'❄️ Winter-Hotspot'}
                        h['auffaelligkeiten'].append(f"{SL[ts]} ({int(sr*100)}% der Meldungen in dieser Jahreszeit)")

            if len(meldungen) >= 3:
                mer = monthend_count / len(meldungen)
                if mer >= 0.5:
                    h['auffaelligkeiten'].append(
                        f"📅 {int(mer*100)}% der Meldungen zum Monatsende — Hinweis auf Umzüge")

                # Monats-Häufung: NUR anzeigen wenn der Monat aktuell relevant ist
                mc = Counter(month_list)
                tm, tmn = mc.most_common(1)[0]
                if tmn / len(month_list) >= 0.4 and len(month_list) >= 3:
                    if tm in naechste_monate:
                        h['auffaelligkeiten'].append(
                            f"📅 Häufung im {MONAT_NAMEN[tm-1]} — jetzt erhöhte Aktivität erwartet")

        if total >= 4:
            uk = len(set(g for g in grp_list if g))
            if uk >= 3:
                h['auffaelligkeiten'].append(
                    f"⚠️ {uk} verschiedene Abfallarten — Standort wird von vielen Verursachern genutzt")

    bezirk_stats = [dict(r) for r in conn.execute("""
        SELECT bezirk, COUNT(*) as total_hotspots, SUM(meldungen_count) as total_meldungen,
               SUM(recurrence_count) as total_recurrence, ROUND(MAX(score),1) as max_score,
               SUM(CASE WHEN score_label='kritisch' THEN 1 ELSE 0 END) as krit,
               SUM(CASE WHEN score_label='hoch' THEN 1 ELSE 0 END) as hoch
        FROM hotspots
        GROUP BY bezirk ORDER BY max_score DESC
    """).fetchall()]
    conn.close()

    # Prognose berechnen — nur zeitlich relevante Muster
    prognose_heute = []
    prognose_woche = []
    prognose_monat = []

    for h in hotspots:
        cid = h['cluster_id']
        meldungen = cluster_m.get(cid, [])
        if len(meldungen) < 5:  # Mindest 5 Meldungen für aussagekräftige Prognose
            continue

        wd_counts = [0]*7
        month_counts = [0]*13
        kw_counts = {}
        parsed = []

        for m in meldungen:
            d = parse_datum(m['datum'])
            if d:
                wd_counts[d.weekday()] += 1
                month_counts[d.month] += 1
                kw = d.isocalendar()[1]
                kw_counts[kw] = kw_counts.get(kw, 0) + 1
                parsed.append(d)

        total = len(parsed)
        if total < 5: continue

        # Wochentag-Wahrscheinlichkeit (heute)
        wd_prob = round(wd_counts[today_wd] / total * 100)

        # Monats-Wahrscheinlichkeit (aktueller Monat)
        month_prob = round(month_counts[today_month] / total * 100)

        # KW-Wahrscheinlichkeit (letzte 3 Jahre gleiche KW)
        kw_vals = [kw_counts.get(today_kw + offset*52, 0) for offset in range(-2, 1)]
        kw_prob = min(round(sum(kw_vals) / max(total, 1) * 100 * 3), 100)

        # Saison-Bonus: wenn aktuelle Saison historisch stark ist
        saison_counts = Counter(get_saison(d.month) for d in parsed)
        saison_ratio = saison_counts.get(today_saison, 0) / total

        # Kombinierter Score für Prognose
        # Gewichtet: Wochentag + Monats-Relevanz + Saison
        combined_prob = round(
            wd_prob * 0.5 +           # 50% Gewicht auf Wochentag
            month_prob * 0.3 +        # 30% auf aktuellen Monat
            saison_ratio * 100 * 0.2  # 20% auf Saison
        )

        top_kat = h.get('top_kategorie', '')
        base = {
            'cluster_id': h['cluster_id'],
            'bezirk': h['bezirk'],
            'strasse': h.get('strasse', ''),
            'plz': h.get('plz', ''),
            'score_label': h['score_label'],
            'meldungen_count': h['meldungen_count'],
            'recurrence_count': h['recurrence_count'],
            'lat_center': h['lat_center'],
            'lon_center': h['lon_center'],
            'top_kat': top_kat,
            'top_kat_label': KATEGORIE_GRUPPEN.get(top_kat, {}).get('label', '') if top_kat else '',
            'top_kat_color': KATEGORIE_GRUPPEN.get(top_kat, {}).get('color', '#888') if top_kat else '#888',
        }

        # Heute: kombinierter Score >= 15%
        if combined_prob >= 15:
            grund_parts = []
            if wd_prob >= 15:
                grund_parts.append(f"{wd_prob}% an {WEEKDAYS_SHORT[today_wd]}")
            if month_prob >= 15:
                grund_parts.append(f"{month_prob}% im {MONAT_NAMEN[today_month-1]}")
            if saison_ratio >= 0.4:
                grund_parts.append(f"{int(saison_ratio*100)}% im {today_saison.capitalize()}")
            grund = " · ".join(grund_parts) if grund_parts else f"{combined_prob}% Wahrscheinlichkeit"
            prognose_heute.append({**base, 'prob': combined_prob, 'grund': grund})

        # Diese Woche: KW-Muster
        if kw_prob >= 15:
            prognose_woche.append({**base, 'prob': kw_prob,
                'grund': f"KW {today_kw}: {kw_prob}% basierend auf Vorjahres-Daten"})

        # Dieser Monat: Monats-Muster
        if month_prob >= 15:
            prognose_monat.append({**base, 'prob': month_prob,
                'grund': f"{month_prob}% aller Meldungen im {MONAT_NAMEN[today_month-1]}"})

    prognose_heute.sort(key=lambda x: (-x['prob'], -x['meldungen_count']))
    prognose_woche.sort(key=lambda x: (-x['prob'], -x['meldungen_count']))
    prognose_monat.sort(key=lambda x: (-x['prob'], -x['meldungen_count']))

    prognose = {
        'heute': prognose_heute[:50],
        'woche': prognose_woche[:50],
        'monat': prognose_monat[:50],
        'datum': today.strftime('%d.%m.%Y'),
        'wochentag': WEEKDAYS_SHORT[today_wd],
        'kw': today_kw,
        'monat_name': MONAT_NAMEN[today_month - 1],
    }

    # Analyse-Daten: Monatliche Aggregation
    monthly = {}
    for row in muell:
        d = parse_datum(row['datum'])
        if not d: continue
        key = d.strftime('%Y-%m')
        if key not in monthly:
            monthly[key] = {'month': key, 'count': 0}
        monthly[key]['count'] += 1

    # Letzte 18 Monate, sortiert
    sorted_months = sorted(monthly.keys())[-18:]
    analyse_monthly = [monthly[m] for m in sorted_months]

    return {
        "hotspots": hotspots,
        "bezirk_stats": bezirk_stats,
        "analyse": {"monthly": analyse_monthly},
        "bezirke": [
            'Charlottenburg-Wilmersdorf','Friedrichshain-Kreuzberg','Lichtenberg',
            'Marzahn-Hellersdorf','Mitte','Neukölln','Pankow','Reinickendorf',
            'Spandau','Steglitz-Zehlendorf','Tempelhof-Schöneberg','Treptow-Köpenick'
        ],
        "kat_keys": {k: {'label': v['label'], 'color': v['color']} for k, v in KATEGORIE_GRUPPEN.items()},
        "prognose": prognose,
        "last_update": datetime.now().strftime("%Y-%m-%d"),
    }

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
