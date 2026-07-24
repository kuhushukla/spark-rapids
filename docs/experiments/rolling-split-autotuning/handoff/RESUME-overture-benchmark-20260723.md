# RESUME — Overture fill-to-target benchmark (local, Spark 3.5.3) — 2026-07-23

Durable checkpoint so this can be resumed any time / anywhere. Read this + the linked docs, then continue at
"NEXT STEP".

## Goal
Benchmark the Overture **scan-heavy segment query** (single scan, no join, aggregates) **WITH vs WITHOUT
fill-to-target** split, in **local mode** on the A5000, to see whether fuller batches cut scan-stage
gpuTime/decode while holding runtime. Same question as the sparkh NDS POC, on a wide nested-column dataset.
`fill-to-target` (ftt) = `ratioBasis=listed`: `maxSplitBytes = batchSize / (decodedBytes/listedBytes)`.

## Where things stand (DONE)
- **353 jar built** = `dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar` (spark353, 2026-07-23 14:43,
  has ratioBasis/floor code; INERT with autotuner off = vanilla scan). **357 preserved** =
  `data/jars/rapids-ratiobasis-357.jar` (the sparkh jar, don't overwrite).
- **Harness written**: `overture_scanheavy.scala` (query + timing loop, `-Dbench.iters`, prints `OVERTURE_ITER i ms`)
  and `run-overture-mpb-sweep.sh` (loops maxPartitionBytes 128m→4g, autotuner OFF, one spark-shell each,
  event logs to `data/overture-mpb-<val>/el`, log to `data/overture-mpb-<val>/run.log`).
- **Data**: `overture_2026-07-22/transportation/type=segment` = **66.3 GiB, 128 parquet files (332–761 MB)**.
- **Env**: `SPARK_HOME=~/Downloads/spark-3.5.3-bin-hadoop3`, `JAVA_HOME=java-1.17.0`, GPU = **A5000**
  `CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504` (**never the T400 GPU-09455c63**).
  local[16], driver 32G, `concurrentGpuTasks=2`, `pinnedPool=8g`, `filecache=false`, `metrics.level=DEBUG`.
- **GPU/Spark runs require sandbox OFF** (sandbox blocks `/dev/nvidia*`; `nvidia-smi` fails inside it).

## Baseline sweep RESULT (DONE 2026-07-23, sandbox off, A5000, local[16], 5 iters, autotuner OFF)
Warm mean ms (iters 2–5): 128m=9165 · 256m=7888 · 512m=6844 · **1g=6644 (OPTIMAL)** · 2g=7405 · 4g=8552.
Clean U-shape; **optimal `maxPartitionBytes=1g`** (~66 tasks ≈ 4 waves), 512m within 3%. All err=0, no OOM.
Logs/event logs: `data/overture-mpb-<val>/run.log` + `.../el/`. **Compare fill-to-target against OFF@1g.**

## DONE (2026-07-23) — full study complete
1. ~~Baseline sweep~~ → optimal 1g.
2. ~~OFF@1g vs fill-to-target~~ → ftt chose 741 MB (`bound_by=ratio`), but **batch max stayed 722 MB byte-identical**
   (split is NOT the fullness lever on array data — capped upstream by read/target caps, `GpuParquetScan.populateCurrentBlockChunk`
   + cuDF chunked reader). ftt ~2% slower than tuned 1g; no GPU-work reduction.
3. ~~Self-tuning check~~ → ftt converges to ~741 MB from any start (128m/1g/4g) → **1.27–1.39× over a mistuned baseline**, −1.7% vs optimum.
4. ~~Write-up~~ → `results/nds-overture-ftt-local-20260723.md` + HTML report `nds-overture-ftt-local-20260723.html`
   (gen `handoff/mpb_writereport_overture.py`). GPU-coverage + data-read sanity captured in the doc.

## Open (optional next)
- Confirm the 722 MB cap is the read/target lever: raise `spark.rapids.sql.batchSizeBytes` / `reader.batchSizeBytes`
  and watch the max batch move (positive control on the actual lever). Arms: fill-to-target `ceiling=8g` and `ceiling=core1`
   (local has few cores → big splits risk under-parallelization; core1 caps to ≥cores tasks). Enable ftt with
   `-Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=<C>` + `spark.rapids.sql.scan.splitAutotuner.historyPath=<local file>`;
   iter1 cold-learns (COLD_START=Spark default), iters 2–5 apply. **Positive control**: confirm `scanMaxSplitBytes`
   actually differs OFF vs ON before trusting any result.

## Design + risks (full design in chat 2026-07-23)
- Query is ~100% scan-dominated (one scan + tiny 1-row final shuffle) — ideal ftt case.
- Floor = `max(64MB, sparkDefault)`; add `-Drapids.autotuner.floor=min` to let sub-default splits through.
- A5000 24 GB: fuller batches × wide array/struct cols → watch GPU OOM at 8g/1-wave; tune concurrentGpuTasks.
- Note [[feedback_real_world_queries]]: this query is a scan probe (no GROUP BY real key); it's the user's given
  query. A real-world GROUP-BY variant would add a shuffle and reduce scan-dominance — different test.

## Prior context (the method this extends)
- sparkh NDS POC: `docs/experiments/rolling-split-autotuning/results/nds-sf3k-scandominance-poc-final-20260722.md`
  (+ HTML report `nds-sf3k-scandominance-report-20260722.html`). Headline: ftt fills batches (15%→63% of target),
  cuts scan/decode/gpuTime, but runtime only wins on scan-dominated queries → gate on `scan_dominance ≥ 0.96`
  (gpuTime −11%, runtime −1.1%). Parser: `handoff/mpb_parse.py`.
- Memory: [[project_mpb_baseline_sweep]], [[project_nds_autotuner_sf3000]], [[reference_env]].
