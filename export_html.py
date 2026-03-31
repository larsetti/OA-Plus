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

KATEGORIE_GRUPPEN = {
    'bauschutt':     {'keywords':['bauschutt','schutt','baumaterial'],'label':'🏗 Bauschutt','color':'#8B4513','hinweis':'Typisch für Gewerbetreibende oder Bauherren — Anzeige empfehlenswert'},
    'gartenabfall':  {'keywords':['gartenabfall','grünschnitt','garten','grün'],'label':'🌿 Gartenabfall','color':'#2d7d2d','hinweis':'Hinweis auf Kleingärten oder Privatgärten in der Nähe'},
    'schrottfahrzeug':{'keywords':['schrottfahrzeug','schrottauto','kfz','fahrzeug'],'label':'🚗 Schrott-KFZ','color':'#555555','hinweis':'Häufig organisierte Ablagerung — Kennzeichen-Kontrolle empfohlen'},
    'sperrmüll':     {'keywords':['sperrmüll','sperr','sofa','matratze','kühlschrank'],'label':'🛋 Sperrmüll','color':'#996600','hinweis':'Oft Privatpersonen, die Sperrmülltermin umgehen'},
    'elektroschrott':{'keywords':['elektroschrott','elektro','e-schrott'],'label':'⚡ Elektroschrott','color':'#0066aa','hinweis':'Entsorgungspflichtige Geräte — Rückgabepflicht besteht'},
    'illegal':       {'keywords':['illegal','ablagerung','wild','schwarze säcke'],'label':'🚮 Illegale Ablagerung','color':'#cc0000','hinweis':'Allgemeine illegale Entsorgung'},
}

def kategorisiere(text):
    t = (text or '').lower()
    for key, grp in KATEGORIE_GRUPPEN.items():
        if any(kw in t for kw in grp['keywords']): return key
    return None

def load_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    muell = conn.execute("SELECT datum,lat,lon,kategorie,betreff FROM meldungen WHERE is_muell=1 AND datum IS NOT NULL AND lat IS NOT NULL").fetchall()
    cluster_m = {}
    for row in muell:
        cid = f"{round(row['lat']/GEO_RADIUS)*GEO_RADIUS:.5f}_{round(row['lon']/GEO_RADIUS)*GEO_RADIUS:.5f}"
        if cid not in cluster_m: cluster_m[cid]=[]
        cluster_m[cid].append({'datum':row['datum'],'kategorie':row['kategorie']or'','betreff':row['betreff']or''})

    hotspots = [dict(r) for r in conn.execute("SELECT * FROM hotspots WHERE meldungen_count >= 3 ORDER BY score DESC").fetchall()]

    for h in hotspots:
        cid = h['cluster_id']
        meldungen = cluster_m.get(cid,[])
        total = len(meldungen) or 1
        grp_list = [g for g in (kategorisiere(m['kategorie']+' '+m['betreff']) for m in meldungen) if g]
        grp_count = Counter(grp_list)
        h['kategorie_mix'] = [{'key':k,'label':KATEGORIE_GRUPPEN[k]['label'],'color':KATEGORIE_GRUPPEN[k]['color'],'hinweis':KATEGORIE_GRUPPEN[k]['hinweis'],'count':c,'pct':round(c/total*100)} for k,c in grp_count.most_common()]
        h['top_kategorie'] = grp_count.most_common(1)[0][0] if grp_count else None
        h['top_kategorie_pct'] = round(grp_count.most_common(1)[0][1]/total*100) if grp_count else 0
        weekdays = []
        for m in meldungen:
            try: weekdays.append(datetime.fromisoformat(m['datum'][:10]).weekday())
            except: pass
        h['weekday_dist']={d:0 for d in WEEKDAYS_SHORT}; h['pattern']='normal'; h['pattern_label']=''; h['auffaelligkeiten']=[]
        if weekdays:
            cnt=Counter(weekdays)
            for i,d in enumerate(WEEKDAYS_SHORT): h['weekday_dist'][d]=cnt.get(i,0)
            tw=len(weekdays); mon_r=cnt.get(0,0)/tw; wknd_r=(cnt.get(5,0)+cnt.get(6,0))/tw
            if mon_r>=0.35 and tw>=2:
                h['pattern']='montag'; h['pattern_label']=f"{int(mon_r*100)}% Montags"
                h['auffaelligkeiten'].append(f"Häufung am Montag ({int(mon_r*100)}%) — Wochenend-Ablagerungen")
            elif wknd_r>=0.35 and tw>=2:
                h['pattern']='wochenende'; h['pattern_label']=f"{int(wknd_r*100)}% Wochenende"
                h['auffaelligkeiten'].append(f"Häufung am Wochenende ({int(wknd_r*100)}%)")
        if h['top_kategorie'] and h['top_kategorie_pct']>=50:
            gi=KATEGORIE_GRUPPEN[h['top_kategorie']]
            h['auffaelligkeiten'].append(f"{gi['label']}: {h['top_kategorie_pct']}% — {gi['hinweis']}")
        if h['recurrence_count']>=3:
            h['auffaelligkeiten'].append(f"Chronischer Ablagerungsort: {h['recurrence_count']}× Wiederkehr")
        if h['pattern']=='montag' and h['top_kategorie']=='gartenabfall':
            h['auffaelligkeiten'].append("🔍 Montags + Gartenabfall → Kleingarten sehr wahrscheinlich")
        elif h['pattern']=='montag' and h['top_kategorie']=='bauschutt':
            h['auffaelligkeiten'].append("🔍 Montags + Bauschutt → Gewerbe nutzt Wochenende zur Entsorgung")
        elif h['top_kategorie']=='schrottfahrzeug' and h['recurrence_count']>=2:
            h['auffaelligkeiten'].append("🔍 Wiederkehrende KFZ-Ablagerung → Kennzeichen-Kontrolle empfohlen")

        # ── Saisonale Analyse ──────────────────────────────────────────
        seasons = []
        month_list = []
        monthend_count = 0
        for m in meldungen:
            try:
                d = datetime.fromisoformat(m['datum'][:10])
                month_list.append(d.month)
                if d.day >= 25: monthend_count += 1
                if d.month in [3,4,5]: seasons.append('frühling')
                elif d.month in [6,7,8]: seasons.append('sommer')
                elif d.month in [9,10,11]: seasons.append('herbst')
                else: seasons.append('winter')
            except: pass

        if seasons:
            season_cnt = Counter(seasons)
            top_season, top_season_n = season_cnt.most_common(1)[0]
            season_ratio = top_season_n / len(seasons)
            SEASON_LABELS = {
                'frühling': ('🌸 Frühlings-Häufung', 'Frühjahrsputz-Effekt — Gartenabfall und Sperrmüll häufen sich März–Mai'),
                'sommer':   ('☀️ Sommer-Häufung',    'Sommerzeit — häufig Gartenabfall, Grillmüll, Umzugssperrmüll'),
                'herbst':   ('🍂 Herbst-Häufung',     'Herbst — Grünschnitt und Gartenabfall nach der Gartensaison'),
                'winter':   ('❄️ Winter-Häufung',     'Wintermonate — oft Sperrmüll und Elektroschrott nach Weihnachten'),
            }
            if season_ratio >= 0.6 and len(seasons) >= 3:
                lbl, hint = SEASON_LABELS[top_season]
                h['auffaelligkeiten'].append(f"{lbl} ({int(season_ratio*100)}%) — {hint}")
            # Kombination Saison + Kategorie
            if top_season in ['frühling','herbst'] and h['top_kategorie'] == 'gartenabfall' and season_ratio >= 0.5:
                h['auffaelligkeiten'].append("🔍 Saison + Gartenabfall → saisonaler Ablagerungspunkt, Kontrolle im Frühjahr/Herbst erhöhen")
            if top_season == 'winter' and h['top_kategorie'] == 'elektroschrott':
                h['auffaelligkeiten'].append("🔍 Winter + Elektroschrott → nach Weihnachten typisch, Aufklärungskampagne sinnvoll")

        # ── Monatsende-Analyse ─────────────────────────────────────────
        if month_list and len(meldungen) >= 3:
            monthend_ratio = monthend_count / len(meldungen)
            if monthend_ratio >= 0.5:
                h['auffaelligkeiten'].append(f"📅 {int(monthend_ratio*100)}% der Meldungen zum Monatsende (ab dem 25.) — Hinweis auf Wohnungswechsel/Umzüge")
            # Monats-Häufung: immer derselbe Monat?
            month_cnt = Counter(month_list)
            top_month, top_month_n = month_cnt.most_common(1)[0]
            if top_month_n / len(month_list) >= 0.5 and len(month_list) >= 3:
                MONTH_NAMES = {1:'Januar',2:'Februar',3:'März',4:'April',5:'Mai',6:'Juni',
                               7:'Juli',8:'August',9:'September',10:'Oktober',11:'November',12:'Dezember'}
                h['auffaelligkeiten'].append(f"📅 Häufung im {MONTH_NAMES[top_month]} — möglicher periodischer Ablagerungsrhythmus")

        # ── Gemischte Kategorien (Schmuggelpunkt) ─────────────────────
        if total >= 4:
            unique_kats = len(set(m['kategorie'] for m in meldungen if m['kategorie']))
            mix_ratio = unique_kats / total
            if mix_ratio >= 0.7 and unique_kats >= 3:
                h['auffaelligkeiten'].append(f"🚨 Hohe Kategorienvielfalt ({unique_kats} verschiedene Müllarten) — bekannter öffentlicher Ablagerungspunkt, viele Verursacher")
            elif unique_kats >= 4:
                h['auffaelligkeiten'].append(f"⚠️ Gemischte Ablagerungen ({unique_kats} Müllarten) — Standort wird von mehreren Personengruppen genutzt")

    bezirk_stats = [dict(r) for r in conn.execute("""
        SELECT bezirk, COUNT(*) as total_hotspots, SUM(meldungen_count) as total_meldungen,
               SUM(recurrence_count) as total_recurrence, ROUND(MAX(score),1) as max_score,
               SUM(CASE WHEN score_label='kritisch' THEN 1 ELSE 0 END) as krit,
               SUM(CASE WHEN score_label='hoch' THEN 1 ELSE 0 END) as hoch
        FROM hotspots GROUP BY bezirk ORDER BY max_score DESC
    """).fetchall()]
    conn.close()

    # ── Prognose berechnen ─────────────────────────────────────────────
    today = datetime.now()
    today_wd = today.weekday()      # 0=Mo, 6=So
    today_month = today.month
    today_day = today.day
    # Kalenderwoche
    today_kw = today.isocalendar()[1]

    WEEKDAY_IDX = {'Mo':0,'Di':1,'Mi':2,'Do':3,'Fr':4,'Sa':5,'So':6}

    prognose_heute = []
    prognose_woche = []
    prognose_monat = []

    for h in hotspots:
        cid = h['cluster_id']
        meldungen = cluster_m.get(cid, [])
        if len(meldungen) < 3:
            continue

        # Wochentag-Wahrscheinlichkeit
        wd_counts = [0]*7
        month_counts = [0]*13
        kw_counts = {}
        day_counts = [0]*32

        for m in meldungen:
            try:
                d = datetime.fromisoformat(m['datum'][:10])
                wd_counts[d.weekday()] += 1
                month_counts[d.month] += 1
                kw = d.isocalendar()[1]
                kw_counts[kw] = kw_counts.get(kw, 0) + 1
                day_counts[d.day] += 1
            except:
                pass

        total = len(meldungen)
        if total == 0:
            continue

        # Wahrscheinlichkeit fuer heute (Wochentag)
        wd_prob = round(wd_counts[today_wd] / total * 100)
        # Wahrscheinlichkeit fuer diesen Monat
        month_prob = round(month_counts[today_month] / total * 100)
        # Wahrscheinlichkeit fuer diese KW (Durchschnitt der letzten 3 Jahre gleiche KW)
        kw_vals = [kw_counts.get(today_kw + offset*52, 0) for offset in range(-2, 1)]
        kw_prob = round(sum(kw_vals) / max(total, 1) * 100 * 3)
        kw_prob = min(kw_prob, 100)

        base = {
            'cluster_id': h['cluster_id'],
            'bezirk': h['bezirk'],
            'strasse': h.get('strasse', ''),
            'score_label': h['score_label'],
            'meldungen_count': h['meldungen_count'],
            'lat_center': h['lat_center'],
            'lon_center': h['lon_center'],
            'top_kat': h.get('top_kategorie', ''),
            'top_kat_label': (KATEGORIE_GRUPPEN.get(h.get('top_kategorie',''), {}).get('label', '') if h.get('top_kategorie') else ''),
            'top_kat_color': (KATEGORIE_GRUPPEN.get(h.get('top_kategorie',''), {}).get('color', '#888') if h.get('top_kategorie') else '#888'),
        }

        if wd_prob >= 20:
            prognose_heute.append({**base, 'prob': wd_prob, 'grund': f"Wochentag-Muster: {wd_prob}% aller Meldungen an {WEEKDAYS_SHORT[today_wd]}"})

        if month_prob >= 15:
            prognose_monat.append({**base, 'prob': month_prob, 'grund': f"Monats-Muster: {month_prob}% aller Meldungen im {today.strftime('%B')}"})

        if kw_prob >= 15:
            prognose_woche.append({**base, 'prob': kw_prob, 'grund': f"KW-Muster: Erhöhte Aktivität in KW {today_kw}"})

    # Sortieren nach Wahrscheinlichkeit, Top 50 je
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
        'monat_name': today.strftime('%B'),
    }

    return {
        "hotspots": hotspots, "bezirk_stats": bezirk_stats,
        "bezirke": sorted(set(h['bezirk'] for h in hotspots if h['bezirk'])),
        "kategorie_gruppen": {k:{'label':v['label'],'color':v['color']} for k,v in KATEGORIE_GRUPPEN.items()},
        "prognose": prognose,
        "last_update": datetime.now().strftime("%Y-%m-%d"),
    }

def main():
    print(f"Lade Daten aus {DB_PATH}...")
    data = load_data()
    print(f"  {len(data['hotspots'])} Hotspots")
    compact = json.dumps(data, ensure_ascii=False, separators=(',',':'))
    tmpl = TEMPLATE.read_text(encoding='utf-8')
    html = tmpl.replace('__APP_DATA_PLACEHOLDER__', compact).replace('__LAST_UPDATE__', data['last_update'])
    OUT_PATH.write_text(html, encoding='utf-8')
    print(f"  Gespeichert: {OUT_PATH}")

if __name__ == "__main__":
    main()
