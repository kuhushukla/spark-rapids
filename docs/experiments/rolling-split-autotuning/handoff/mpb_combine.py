#!/usr/bin/env python3
# Combine per-query CSVs across maxPartitionBytes configs into one cross-config view.
# Reads data/mpb-perquery-<cfg>.csv for each cfg present; emits a per-query warm-time matrix
# (+ scan/decode/fullness at the two ends) and headline movers. Writes data/mpb-perquery-all.csv.
import csv, os, sys

CFGS = ["128m","256m","512m","1g","2g","4g"]
BASE = "/home/kuhu/Reps/spark-rapids/data"

def load(cfg):
    p=f"{BASE}/mpb-perquery-{cfg}.csv"
    if not os.path.exists(p): return None
    d={}
    with open(p) as f:
        for r in csv.DictReader(f):
            d[r["query"]]={k:float(v) if k not in("query",) else v for k,v in r.items()}
    return d

data={c:load(c) for c in CFGS}
have=[c for c in CFGS if data[c]]
queries=sorted(next(iter(d for d in data.values() if d)).keys())

rows=[]
for q in queries:
    warm={c:data[c][q]["warm_ms"] for c in have if q in data[c]}
    if not warm: continue
    w0=warm.get("128m"); best_c=min(warm,key=warm.get); best=warm[best_c]
    spd = (w0/best) if (w0 and best) else 0
    rows.append((q,warm,w0,best_c,best,spd))

# full matrix CSV
out=f"{BASE}/mpb-perquery-all.csv"
with open(out,"w",newline="") as f:
    w=csv.writer(f)
    w.writerow(["query"]+[f"warm_{c}_ms" for c in have]+["best_cfg","speedup_128m_to_best",
        "scan128_s","scan_best_s","pct_full_128m","pct_full_best"])
    for q,warm,w0,best_c,best,spd in rows:
        s128=data["128m"][q]["scan_s"] if data.get("128m") else 0
        sb=data[best_c][q]["scan_s"]
        p128=data["128m"][q]["pct_target"] if data.get("128m") else 0
        pb=data[best_c][q]["pct_target"]
        w.writerow([q]+[round(warm.get(c,0)) for c in have]+[best_c,round(spd,2),
            round(s128,1),round(sb,1),round(p128,1),round(pb,1)])

print(f"configs: {have}   queries: {len(rows)}   -> {out}")
tot={c:sum(data[c][q]["warm_ms"] for q in queries if q in data[c])/1000 for c in have}
print("WARM total by config (s):", {c:round(tot[c],1) for c in have})
print("\n== biggest absolute movers (128m warm - best warm), top 15 ==")
movers=sorted(rows,key=lambda r:-(r[2]-r[4]) if r[2] else 0)[:15]
print(f"{'query':14s}"+"".join(f"{c:>8s}" for c in have)+f"{'best':>7s}{'spd':>6s}")
for q,warm,w0,best_c,best,spd in movers:
    print(f"{q:14s}"+"".join(f"{round(warm.get(c,0)):>8d}" for c in have)+f"{best_c:>7s}{spd:>6.2f}")
print("\n== any WARM regressions at the largest split (4g or 2g slower than its best)? ==")
for q,warm,w0,best_c,best,spd in rows:
    big = warm.get("4g") or warm.get("2g")
    if big and best and big > best*1.10:
        print(f"  {q:14s} best {best:.0f}ms @{best_c}  vs {round(big)}ms @large  (+{(big/best-1)*100:.0f}%)")
