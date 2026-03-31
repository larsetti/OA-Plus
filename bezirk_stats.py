import sqlite3
from collections import defaultdict

conn = sqlite3.connect(r'C:\Users\larsw\OneDrive\OA+\muell-monitor\ordnungsamt.db' if False else 'ordnungsamt.db')
rows = conn.execute('SELECT bezirk, score FROM hotspots WHERE meldungen_count >= 3 ORDER BY bezirk, score').fetchall()

bezirk_scores = defaultdict(list)
for bezirk, score in rows:
    bezirk_scores[bezirk].append(score)

for bezirk, scores in sorted(bezirk_scores.items()):
    n = len(scores)
    p50 = scores[int(n*0.50)]
    p75 = scores[int(n*0.75)]
    p90 = scores[min(int(n*0.90), n-1)]
    print(f'{bezirk}: n={n}, p50={p50:.1f}, p75={p75:.1f}, p90={p90:.1f}, max={scores[-1]:.1f}')
