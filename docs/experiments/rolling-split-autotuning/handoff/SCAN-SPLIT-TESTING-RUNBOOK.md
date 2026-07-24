# Scan-split testing runbook (portable) — for any Spark + RAPIDS-GPU environment

**Audience:** another Claude session (or engineer) that must benchmark how the Parquet **scan split size**
(`spark.sql.files.maxPartitionBytes`) and the RAPIDS **fill-to-target autotuner** affect a set of queries on a
dataset — and plot the results. Self-contained: the harness, runner, parser and plotter are all inline below.

**What you produce per query:** a `maxPartitionBytes` sweep (autotuner OFF), the autotuner (ON) converged split, an
ON-vs-OFF **goal analysis** (does it save gpuTime + GPU-decode while keeping wall within 5%?), and **two plots**
(wall vs split, and gpuTime/scan/decode vs split).

---

## 0. Grounding rules (do not skip — these are why past runs were wrong)

1. **Verify, don't assume.** Every claim about behaviour must come from source or a controlled probe. Before trusting
   any "X has no effect / X is the bottleneck", run a **positive control** that shows the knob moves the metric.
2. **One query per Spark session.** Never run multiple queries in one `spark-shell`. They share page-cache and (with
   the autotuner) share per-table history — cross-contaminating results. Loop query × config, fresh session each.
3. **5 iterations; drop iter 1.** Iter 1 is cold (and for the autotuner it is `COLD_START` = no history → default
   split). Report the **mean of warm iters 2–5**. Compare **warm-to-warm** only.
4. **GPU coverage check is mandatory.** Confirm the query runs on GPU from the **post-AQE plan** in the event log,
   with **zero** `cannot run on GPU`. `.explain()` on an un-executed DataFrame shows the pre-AQE **CPU** plan — do not
   trust it.
5. **Never run two GPU Spark jobs at once on one GPU.** They fight for GPU memory → `pool allocation -NNN MiB` OOM.
   Serialize all GPU runs.
6. **`spark.eventLog.dir` must be an ABSOLUTE `file:` URI.** A relative path makes `URI.getPath()` null → SparkContext
   init crash before any query runs. Always `file:$PWD/...` or `file:/abs/...`.
7. **Watch disk for shuffle-heavy queries.** `COUNT(DISTINCT ...)` / big joins spill tens of GB to local disk and can
   fill it (crashing with `No space left on device`). Poll `df` during the run; if a query needs exact distinct over
   100M+ rows, use `approx_count_distinct(...)` (GPU HyperLogLog) instead. **Never delete files you didn't create to
   free space — report large dirs and ask.**
8. **The exit-1 `BlockManagerId ... executorId() null` NPE at shutdown is benign** (local-mode race). Judge success by
   the presence of the `ITER` completion lines, not the exit code alone.

---

## 1. Discover & confirm the environment (ask the user if unknown)

Fill these in for the target machine. **Do not guess** — discover with the probes, and confirm anything ambiguous.

| var | what | how to discover |
|---|---|---|
| `SPARK_HOME` | Spark install | `echo $SPARK_HOME`; must match the RAPIDS jar's Spark version |
| `JAVA_HOME` | JDK (17 for Spark 3.5+) | `ls /usr/lib/jvm` |
| `GPU_UUID` | the exact GPU to use | `nvidia-smi -L` → pick the intended one (e.g. an A5000, **not** a small display GPU); set `CUDA_VISIBLE_DEVICES=GPU-<uuid>` |
| `CORES` | executor parallelism | local: `local[N]` (N = physical cores − 2); cluster: `#executors × cores`. **This sets the task-count floor for the goal analysis** |
| `DRIVER_MEM` | driver heap | e.g. `32G` local |
| `JAR` | RAPIDS jar **with the autotuner built in** | `ls dist/target/rapids-4-spark_*-cuda*.jar`; confirm its `buildver` matches Spark |
| `SHUFFLE_MGR` | must match buildver | `com.nvidia.spark.rapids.spark<ver>.RapidsShuffleManager` (e.g. `spark353` for 3.5.3, `spark357` for 3.5.7) |
| `DATA_BASE` | dataset root (read-only) | user-supplied; verify with `ls`/`du -sh` |
| `OUT_BASE` | where run dirs go (writable, lots of space) | e.g. `$PWD/data/scan-split-runs`; verify free space with `df -h` |

Positive control that the autotuner code is present in the jar:
```bash
grep -c "spark.rapids.sql.scan.splitAutotuner.historyPath" <(unzip -p "$JAR" 'com/nvidia/spark/rapids/RapidsConf*.class' 2>/dev/null) 2>/dev/null || \
  echo "if 0/err: confirm the jar was built from a branch that has ScanSplitAutotuner (fill-to-target). Without it, do OFF-only."
```
If the autotuner is absent, run the **sweep + plots only** (skip §4/§5); everything else still works.

**Questions to ask the user before starting** (don't assume):
- Which GPU, and how many cores, on this machine?
- Dataset path(s) and the SQL queries (or the questions to turn into scan-heavy queries)?
- Is the jar built with the fill-to-target autotuner, and for which Spark `buildver`?
- Any shared/production data that is **read-only / must never be written or deleted**?

---

## 1b. Porting the dataset to a new machine

**Scan-split behaviour depends on the on-disk file-size distribution** (task packing, and the file-alignment "skew
bump" when split ≈ median file size). So *how* you get the data matters — a differently-partitioned copy gives
different numbers.

- **To reproduce an existing study → COPY the exact directory** (same files, sizes, layout). Do **not** re-download.
- **For a brand-new experiment → download fresh from source** (accept a different file layout; re-point view paths).

**Minimal move set:** (1) the **buildver-matching jar** (self-contained — bundles cuDF native `.so` + classes; a jar
built with only a `spark353` shim runs **only** on Spark 3.5.3, a `spark357` jar only on 3.5.7, etc.), (2) **this
runbook**, (3) the **data**. Plus, already on the target: Spark (matching version), JDK 17, a CUDA-12 GPU + driver,
Python 3 (the parser is stdlib-only), and `matplotlib` (only for plotting). Nothing else from the source repo is needed.

**Copy the data (fastest, reproducible)** — parquet is already compressed, so do **not** re-compress. All of these only
READ the source; write to a fresh empty dest:
```bash
# resumable (best default) — push from source machine:
rsync -aP --no-compress /SRC/<dataset_dir>/ USER@HOST:/DEST/<dataset_dir>/
# single-stream, often faster for the initial bulk (few large files):
tar -C /SRC -cf - <dataset_dir> | ssh USER@HOST 'tar -C /DEST -xf -'
# same host / mounted disk:
cp -a /SRC/<dataset_dir> /DEST/
# via cloud bucket when there's no direct SSH:
aws s3 cp --recursive /SRC/<dataset_dir> s3://BUCKET/<dataset_dir>   # then aws s3 cp --recursive back on target
```

**Download fresh from source (different data)** — e.g. Overture Maps is public on S3; **pick an explicit release**, and
note its native layout is `theme=<theme>/type=<type>/…` (adjust the `bench.scala` view paths to match):
```bash
aws s3 ls --no-sign-request s3://overturemaps-us-west-2/release/        # choose a release tag
REL=<release-tag>
aws s3 sync --no-sign-request s3://overturemaps-us-west-2/release/$REL/theme=transportation/ /DEST/overture/theme=transportation/
# (or: pip install overturemaps ; overturemaps download ...)
```

> Layout caveat: this study's local copy used a repartitioned `<theme>/type=<type>/` layout (fewer, larger files) that
> is **not** identical to Overture-native (`theme=<theme>/type=<type>/`, more/smaller files). Same rows, different file
> sizes → different scan-split optima. Verify the file-size distribution on the target before comparing to prior runs:
> `find /DEST/<dataset_dir> -name '*.parquet' | wc -l` and `du -sh /DEST/<dataset_dir>/*/`.

**Never delete source data to make room on the target** — if the target is short on space, report what's large and ask;
do not `rm`/`truncate` anything you didn't create.

---

## 2. Author the queries (scan-heavy, real, GPU-covered)

Each query must be a **genuine analytical question** (never a synthetic `SELECT sum(1)` scan probe) and **scan-heavy**:
one big Parquet scan + a *small* `GROUP BY` on a low-cardinality key, so scan+decode dominate and only tiny aggregates
shuffle. Make it scan-heavy by adding real projected columns + GROUP BY a real key — not meaningless aggregates.

Watch for **null-artifact traps**: `size(NULL) = -1`, so `AVG(size(col))` goes negative when the array is often
absent. Use `size(col) > 0` guards, and if you report an approximate metric (`approx_count_distinct`) say it is noisy
(±1–2%, can go slightly negative) — only large values are meaningful.

Save the harness below as `bench.scala` and edit the two marked sections (views + queries).

```scala
// bench.scala — one-query-per-session timing harness. Prints:  ITER <query> <iter> <ms>  rows=<n>
// Driven by:  spark-shell -i bench.scala   with -Dbench.query=<name> -Dbench.iters=N [-Dbench.explain=true]
val BASE = sys.props.getOrElse("bench.base", "/ABS/PATH/TO/DATA")   // or hardcode

// ---- EDIT 1: register your dataset views ----
Seq(
  "transportation/type=segment" -> "segment",
  "places/type=place"           -> "places"
  // ... add the tables your queries scan ...
).foreach { case (p, v) => spark.read.parquet(s"$BASE/$p").createOrReplaceTempView(v) }

// ---- EDIT 2: your named, scan-heavy queries ----
val queries = Map(
  "q1" -> """SELECT class, COUNT(*) AS n,
               ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_named
             FROM segment WHERE class IS NOT NULL GROUP BY class ORDER BY n DESC"""
  // ... more queries ...
)

val which = sys.props.getOrElse("bench.query", queries.keys.head)
val M     = sys.props.getOrElse("bench.iters", "5").toInt
val doEx  = sys.props.getOrElse("bench.explain", "false").toBoolean
for (name <- (if (which == "all") queries.keys.toSeq.sorted else Seq(which))) {
  val q = queries(name)
  if (doEx) { println(s"########## EXPLAIN $name ##########"); spark.sql(q).explain() }
  for (i <- 1 to M) {
    val t0 = System.nanoTime()
    val n  = spark.sql(q).collect().length
    val ms = (System.nanoTime() - t0) / 1e6
    println(f"ITER $name%s $i%d ${ms}%.0f  rows=$n%d")
  }
}
System.exit(0)
```

**Smoke test first** (1 iter, +explain, one query) and verify GPU coverage before any sweep — see §6 check A.

---

## 3. Sweep: `maxPartitionBytes` with the autotuner OFF

Save as `sweep.sh`. Loops query × config, one session each, absolute event-log dir. Grid `{256m,512m,1g,2g,4g}`
(add `128m` when the small end matters, e.g. wall keeps falling toward 256m).

```bash
#!/usr/bin/env bash
set -uo pipefail
: "${SPARK_HOME:?}"; : "${JAVA_HOME:?}"; : "${JAR:?}"; : "${SHUFFLE_MGR:?}"
: "${CUDA_VISIBLE_DEVICES:?set to GPU-<uuid>}"; : "${CORES:=16}"; : "${DRIVER_MEM:=32G}"
BENCH="${BENCH:-$PWD/bench.scala}"; OUT_BASE="${OUT_BASE:-$PWD/data/scan-split-runs}"
QUERIES="${QUERIES:?space-separated names}"; ITERS="${ITERS:-5}"; CONFIGS="${CONFIGS:-256m 512m 1g 2g 4g}"
for Q in $QUERIES; do for MPB in $CONFIGS; do
  OUT="$OUT_BASE/$Q-$MPB"; EL="$OUT/el"; mkdir -p "$EL"
  echo "########## $Q maxPartitionBytes=$MPB ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master "local[$CORES]" --driver-memory "$DRIVER_MEM" \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$MPB \
    --conf spark.rapids.sql.metrics.level=DEBUG --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager="$SHUFFLE_MGR" \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    --driver-java-options "-Dbench.base=$DATA_BASE -Dbench.query=$Q -Dbench.iters=$ITERS" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$BENCH" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $Q $MPB: $(grep -oE "ITER $Q [0-9]+ [0-9]+" "$OUT/run.log" | awk '{print $4}' | tr '\n' ' ')"
done; done
echo "SWEEP DONE $(date +%H:%M:%S)"
```
Run in the background; **poll `df -h` in parallel** and stop if free space drops near a few GB.
On a shared cluster, re-verify `#executors × cores` is identical every run.

---

## 4. Autotuner ON (fill-to-target)

The autotuner sizes the split per table so each task decodes ~one GPU batch:
`split = clamp(batchSize / (decodedBytes/listedBytes), floor, ceiling)`. Enabled by giving it a **history file**;
tuned by JVM `-D` flags (verified in `RapidsConf.scala:690` and `ScanSplitAutotuner.scala:228/261/282`):

- `--conf spark.rapids.sql.scan.splitAutotuner.historyPath=$OUT/history.tsv` — **enables** it (empty = disabled).
- `-Drapids.autotuner.ratioBasis=listed` — fill-to-target (default `listed`).
- `-Drapids.autotuner.ceiling=8g` — cap; `core<N>` caps to ≥N tasks/core (parallelism floor for big clusters), `8g`/`<N>g` = flat cap, `none` = uncapped.
- `-Drapids.autotuner.floor=min` — let a ratio-driven split fall below Spark's default (else floored at maxPartitionBytes).

**Own `history.tsv` per (query, start)** — never shared (tables scanned by multiple queries would cross-contaminate).
Run from **≥2 starting `maxPartitionBytes`** (e.g. 128m and 4g) to prove convergence is start-independent. Save as
`ftt.sh` — identical to `sweep.sh` except the loop is over `START in 128m 4g`, add the two conf/`-D` lines above with
`HIST="$OUT/history.tsv"`, and set `--conf spark.sql.files.maxPartitionBytes=$START`. The converged split is in the
`DECIDED` log lines: `grep -oE 'split_bytes=[0-9]+ .*bound_by=[a-z_]+' "$OUT/run.log" | tail -1`.

---

## 5. Parse warm metrics

Save as `parse_warm.py`. One run dir = one query × N iters. Attributes tasks to the scan stage, drops iter 1, averages
warm 2–5. Emits tasks, avg/max batch, **scan time, GPU decode, gpuTime** (`gpuTime` = GPU-semaphore-holding time,
`GpuTaskMetrics.scala:331`), **byte skew** (`max÷median bytes-read per scan task` — a straggler indicator), scanned
bytes, and wall (from `ITER` lines).

```python
#!/usr/bin/env python3
import json, glob, sys, re, statistics
from collections import defaultdict
SCAN={"scan time","GPU decode time","output columnar batches","sum of output GPU batch bytes",
      "maximum output GPU batch bytes per task"}
def pv(v):
    if v is None: return None
    s=str(v)
    if ":" in s:
        try: h,m,r=s.split(":"); return int((int(h)*3600+int(m)*60+float(r))*1e9)
        except: return None
    try: return int(s)
    except: return None
def wall(run_log):
    its=[(int(a),int(b)) for a,b in re.findall(r"ITER \w+ (\d+) (\d+)", open(run_log,encoding="utf-8",errors="replace").read())]
    warm=[ms for i,ms in sorted(its) if i>=2]
    return statistics.mean(warm) if warm else (sorted(its)[0][1] if its else 0)
def parse(rundir):
    el=[e for e in glob.glob(f"{rundir}/el/*") if "inprogress" not in e]
    if not el: return None
    acc={}; acc_ex={}; stage_ex={}; order=[]; seen=set()
    pe=defaultdict(lambda: defaultdict(int)); pmax=defaultdict(int); pgpu=defaultdict(int)
    ptasks=defaultdict(int); pbytes=defaultdict(list)
    def walk(n,eid):
        if "Scan" in n.get("nodeName",""):
            for me in n.get("metrics",[]):
                if me.get("name") in SCAN: acc[me["accumulatorId"]]=me["name"]; acc_ex[me["accumulatorId"]]=eid
        for c in n.get("children",[]): walk(c,eid)
    for line in open(el[0],encoding="utf-8",errors="replace"):
        if 'SparkListenerSQLExecutionStart' in line or 'SparkListenerSQLAdaptiveExecutionUpdate' in line:
            e=json.loads(line); eid=e.get("executionId")
            if eid is not None and e.get("sparkPlanInfo"):
                walk(e["sparkPlanInfo"],eid)
                if eid not in seen: seen.add(eid); order.append(eid)
        elif 'SparkListenerJobStart' in line:
            e=json.loads(line); eid=e.get("Properties",{}).get("spark.sql.execution.id")
            if eid is not None:
                eid=int(eid)
                for sid in e.get("Stage IDs",[]): stage_ex[sid]=eid
        elif '"Accumulables"' in line and "SparkListenerTaskEnd" in line:
            e=json.loads(line); sid=e.get("Stage ID"); eid=stage_ex.get(sid)
            rb=e.get("Task Metrics",{}).get("Input Metrics",{}).get("Bytes Read"); isc=False
            for a in e.get("Task Info",{}).get("Accumulables",[]):
                aid=a.get("ID"); nm=a.get("Name")
                if aid in acc:
                    v=pv(a.get("Update"))
                    if v is None: continue
                    isc=True; ex=acc_ex[aid]; mn=acc[aid]
                    if mn=="maximum output GPU batch bytes per task": pmax[ex]=max(pmax[ex],v)
                    else: pe[ex][mn]+=v
                elif nm=="gpuTime" and eid is not None:
                    v=pv(a.get("Update"))
                    if v is not None: pgpu[eid]+=v
            if isc and eid is not None:
                ptasks[eid]+=1
                if rb: pbytes[eid].append(rb)
    q=[e for e in order if pe[e].get("output columnar batches",0)>0]
    warm=q[1:] if len(q)>1 else q
    n=len(warm); agg=defaultdict(int); mx=gpu=tasks=0; allb=[]
    for e in warm:
        for k,v in pe[e].items(): agg[k]+=v
        mx=max(mx,pmax[e]); gpu+=pgpu[e]; tasks+=ptasks[e]; allb+=pbytes[e]
    nb=agg.get("output columnar batches",0); ob=agg.get("sum of output GPU batch bytes",0); allb.sort()
    return dict(iters=n, tasks=tasks/n if n else 0, avg=ob/nb if nb else 0, maxb=mx,
        scan=agg.get("scan time",0)/1e9/n if n else 0, decode=agg.get("GPU decode time",0)/1e9/n if n else 0,
        gpu=gpu/1e9/n if n else 0, scanned=sum(allb)/n if n else 0,
        skew=(allb[-1]/allb[len(allb)//2]) if allb else 0)
if __name__=="__main__":
    print("rundir\tit\ttasks\tskew\tscannedGiB\tavgMB\tscan_s\tdecode_s\tgpu_s\twall_ms")
    for d in sys.argv[1:]:
        r=parse(d)
        if not r: print(d, "no-el"); continue
        w=wall(f"{d}/run.log"); tag=d.rstrip('/').split('/')[-1]
        print(f"{tag}\t{r['iters']}\t{r['tasks']:.0f}\t{r['skew']:.2f}\t{r['scanned']/2**30:.2f}\t{r['avg']/2**20:.0f}\t{r['scan']:.1f}\t{r['decode']:.1f}\t{r['gpu']:.1f}\t{w:.0f}")
```
Run: `python parse_warm.py $OUT_BASE/q1-* > q1_sweep.tsv` (sweep) and again for the `q1-ftt-*` dirs. The scanned bytes
should be **identical across configs** for a given query (same columns read) — if not, the parse is wrong.

---

## 6. Mandatory checks (per query, before trusting numbers)

- **A. GPU coverage.** Parse the post-AQE plan for GPU ops and confirm 0 fallbacks:
  ```bash
  python3 -c "import json,glob,re,collections as C; el=[e for e in glob.glob('OUT/el/*') if 'inprogress' not in e][0];
  c=C.Counter();
  [ [c.update(re.findall(r'Gpu[A-Za-z]+', n.get('nodeName',''))) or [w(k) for k in n.get('children',[])] for n in [j['sparkPlanInfo']]]
    for j in (json.loads(l) for l in open(el) if 'AdaptiveExecutionUpdate' in l) if j.get('sparkPlanInfo') ]"
  grep -c 'cannot run on GPU' OUT/run.log     # must be 0 (run once with --conf spark.rapids.sql.explain=NOT_ON_GPU)
  ```
  Expect `GpuScan`, `GpuHashAggregate`, etc. If key ops fall back to CPU, fix the query or note it — the scan-split
  story only holds when the scan is on GPU.
- **B. Positive control that the knob moves the metric.** Across the sweep, **task count must change** (≈ listedBytes ÷
  maxPartitionBytes) and avg batch must grow with split. If task count is identical across configs, the split isn't
  taking effect (or the data can't subdivide further — a real "flat/saturated" finding, state it).
- **C. Non-monotonic wall in a shallow noisy valley?** If configs land within run-to-run noise (~5–10%) and the
  ordering looks non-physical, cancel drift with an **interleaved probe**: one session, N rounds, every config
  back-to-back each round (set `spark.sql.files.maxPartitionBytes` per query at planning), report the r2–N mean.
- **D. ftt start-independence.** The two starts (128m, 4g) must converge to the **same** `split_bytes`.

---

## 7. Plot (always — the shapes are the story)

Two plots per query. Save as `plot.py`; feed it the sweep TSV from §5.

```python
#!/usr/bin/env python3
# Usage: python plot.py q1_sweep.tsv q1  [ftt_mb ftt_wall_ms ftt_gpu_s ftt_scan_s ftt_decode_s]
import sys, matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
tsv, name = sys.argv[1], sys.argv[2]
MB={"128m":128,"256m":256,"512m":512,"1g":1024,"2g":2048,"4g":4096}
rows=[]
for ln in open(tsv):
    p=ln.rstrip("\n").split("\t")
    if p[0]=="rundir" or len(p)<10: continue
    cfg=p[0].split("-")[-1]
    if cfg not in MB: continue
    rows.append((MB[cfg], float(p[6]), float(p[7]), float(p[8]), float(p[9])))  # mb,scan,decode,gpu,wall
rows.sort()
xs=[r[0] for r in rows]
fig,(a1,a2)=plt.subplots(1,2,figsize=(12,4.6),dpi=140)
# left: wall vs split (the runtime U/W-curve)
a1.plot(xs,[r[4]/1000 for r in rows],"-o",color="#2a78d6",lw=2)
mi=min(rows,key=lambda r:r[4]); a1.scatter([mi[0]],[mi[4]/1000],s=140,facecolors="none",edgecolors="#2a78d6",lw=2)
a1.set_title(f"{name}: warm wall vs split (optimum ringed)")
a1.set_ylabel("warm wall (s)")
# right: GPU work vs split (gpuTime/scan/decode decay)
for idx,lab,col in [(3,"gpuTime","#2a78d6"),(1,"scan","#eb6834"),(2,"decode","#1baf7a")]:
    a2.plot(xs,[r[idx] for r in rows],"-o",color=col,lw=2,label=lab)
a2.set_title(f"{name}: GPU work vs split"); a2.legend(frameon=False)
if len(sys.argv)>=8:  # overlay ftt point
    fmb,fw,fg=float(sys.argv[3]),float(sys.argv[4]),float(sys.argv[5])
    a1.scatter([fmb],[fw/1000],s=130,marker="D",color="#1baf7a"); a2.scatter([fmb],[fg],s=130,marker="D",color="#1baf7a")
for ax in (a1,a2):
    ax.set_xscale("log",base=2); ax.set_xticks(list(MB.values())); ax.set_xticklabels(list(MB.keys()))
    ax.set_xlabel("maxPartitionBytes (log)"); ax.grid(True,color="#ececea")
    for s in ("top","right"): ax.spines[s].set_visible(False)
fig.tight_layout(); out=f"{name}_scan-split.png"; fig.savefig(out); print("wrote",out)
```

**Read the shapes:** `gpuTime` and `decode` almost always **decay** with bigger splits (fuller batches → less GPU
work). `wall` follows its own curve — a **U** (one optimum), a **W** (twin optima + a file-alignment *skew bump* where
split ≈ median file size), **flat** (data can't subdivide → knob saturates), or **rising** (heavy per-task query,
smaller splits win on parallelism). **Lower gpuTime ≠ faster** — that divergence between the two plots is the point.

---

## 8. Goal analysis (ON vs OFF)

For each query, compare **ftt (ON)** to the **sweep optimum (OFF)** — the strictest baseline:

| metric | pass condition |
|---|---|
| gpuTime saved | `ftt.gpu < optimum.gpu` (efficiency) |
| decode saved | `ftt.decode < optimum.decode` |
| runtime | `(ftt.wall − optimum.wall) / optimum.wall ≤ 5%` |

Also report ftt vs each *fixed* config (`Δ wall / Δ scan / Δ gpuTime = ftt − baseline`) — against an **untuned
default** the autotuner usually wins outright. Interpretation guide (observed across datasets):

- **Reads a lot** (high read-selectivity → high decoded/listed) → ftt sizes **down** → lands on the optimum. Goal met.
- **Reads few, highly-compressible columns** (low decoded/listed) → ftt sizes **up** → fewer tasks than cores →
  saves GPU work but **loses 5–11% wall** (parallelism-bound). Goal met on GPU metrics, missed on the ≤5% wall vs the
  hand-tuned optimum.
- **Multi-table union** → ftt sizes **each table separately** → collapses cross-table byte skew a single global
  `maxPartitionBytes` cannot. Its standout win.
- **Data can't subdivide** (few files, ≤ ~1 task/core at large split) → flat; ftt does no harm.

---

## 9. Output artifacts

Per dataset produce: one **index** page/table (cross-query summary + the goal table), and one **report per query**
(question + SQL + result + data-read facts + the two plots + sweep table + ftt convergence + ftt-vs-baseline diffs +
warm-to-warm). Keep every metric **measured** (from event logs), state the config, and flag any approximate/ artifact
value honestly. Reference implementation to copy structure from: `docs/experiments/rolling-split-autotuning/results/
nds-overture-rw2-index-20260723.html` and the per-query `nds-overture-{rw6..rw9,gf1,gf2}-*.html`.
```
