# Standardized scan-split benchmark method (local Spark 3.5.3 + RTX A5000)

The one source of truth for how every Overture / scan-split benchmark in this experiment is run. Follow it exactly
so results across queries and dates are comparable. Deviating (e.g. lumping queries into one session) invalidates
warm-to-warm and autotuner comparisons.

## Hard invariants (never change between runs)
- **GPU:** RTX **A5000 only** — `CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504`. **Never the T400.**
- **Sandbox OFF** for any Spark/GPU run (GPU + local port binding need it). Read-only parsing runs sandboxed.
- **Spark 3.5.3**: `SPARK_HOME=~/Downloads/spark-3.5.3-bin-hadoop3`, `JAVA_HOME=java-1.17.0-openjdk-amd64`.
- **Jar**: `dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar` built with **buildver=353** (verify date).
  Shuffle manager must match: `com.nvidia.spark.rapids.spark353.RapidsShuffleManager`.
- **Fixed confs (identical every run):** `--master local[16]`, driver 32G, `maxResultSize=2GB`,
  `concurrentGpuTasks=2`, `memory.pinnedPool.size=8g`, `filecache.enabled=false`,
  `spark.rapids.sql.metrics.level=DEBUG`, `spark.rapids.sql.explain=NONE` (for runs; use `NOT_ON_GPU` only when
  auditing GPU coverage). The **only** knob that varies is `spark.sql.files.maxPartitionBytes`.

## The rules that make results comparable
1. **ONE QUERY PER SESSION.** Never run multiple queries in a single spark-shell. Mixing them shares page-cache and
   (with the autotuner) shares per-table history — e.g. rw6/rw7/rw8 all scan `segment`, so one session would
   cross-contaminate their learned ratios and break the clean COLD_START→warm progression.
2. **Own run dir per (query, config):** `data/overture-<suite>-<query>-<config>/` with its own `el/` event-log dir.
3. **5 iterations. iter1 = COLD.** For the autotuner, iter1 = **COLD_START** (no history → default split). Always
   **drop iter1** and report the mean of **warm iters 2–5**. Compare **warm-to-warm** only.
4. **Autotuner (fill-to-target) gets its OWN `history.tsv` per (query, start):**
   `--conf spark.rapids.sql.scan.splitAutotuner.historyPath=$OUT/history.tsv`, plus
   `-Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g -Drapids.autotuner.floor=min`.
   Run from ≥2 starts (128m, 4g) to prove convergence is start-independent. History file is never shared across queries.
5. **Sweep grid:** `maxPartitionBytes ∈ {256m, 512m, 1g, 2g, 4g}` (add 128m when the low end matters).
6. **Shallow-valley tie-break:** when configs land within run-to-run noise (~5–10%), a plain sequential sweep can
   look non-monotonic from thermal/cache/JVM drift. Confirm ordering with a **drift-cancelled interleaved probe**
   (one session, N rounds, all configs back-to-back each round, `maxPartitionBytes` set per query at planning) —
   `overture_interleaved.scala`. Report the interleaved r2–N mean for wall time; verify iter1 isn't a cold outlier.

## Gotchas
- **`spark.eventLog.dir` MUST be an absolute `file:` URI.** A relative path (`file:data/foo/el`) makes
  `URI.getPath()` null → SparkContext init crashes at `EventLogFileWriters.scala:77` before any query runs. Always
  prefix with `$PWD`/`$REPO` (`file:$PWD/data/...`).
- The exit-1 `BlockManagerId ... executorId() null` NPE at shutdown is a benign local-mode race, not a failure —
  check for `GF_ITER`/`RW2_ITER` completion lines, not the exit code alone.

## Mandatory grounding checks (every suite, before trusting numbers)
- **GPU coverage:** post-AQE plan (`SparkListenerSQLAdaptiveExecutionUpdate` in the event log) shows `GpuScan`,
  `GpuHashAggregate`, `GpuGenerate`, etc.; driver log has **zero** `cannot run on GPU`. (`.explain()` on an
  un-executed DF shows `isFinalPlan=false` = the pre-AQE CPU plan — do NOT trust it for GPU coverage.)
- **Parallelism constant:** confirm `local[16]` / executor-core count is identical across runs (shared-cluster runs
  must re-verify exec×core every time).
- **No file destruction:** runs only READ the Overture parquet and WRITE new `data/overture-*` dirs. Never delete.

## Runners & parsers (this suite)
- Query harness: `overture_rw2_bench.scala` (`-Dbench.query=rw6|rw7|rw8|rw9`, `-Dbench.iters=N`, `-Dbench.explain`).
- Sweep: `run-rw2-sweep.sh` (loops query × config, one session each).
- Autotuner: `run-rw2-ftt.sh` (loops query × start, own history.tsv each).
- Warm metrics parser: `rw2_warm_parse.py` — attributes tasks to SQL exec, drops iter1, averages warm 2–5; emits
  tasks / avg+max batch / scan time / GPU decode / gpuTime / **byte skew** (`max÷median bytes read per scan task`) / wall.
- Interleaved tie-break: `overture_interleaved.scala` (+ python per-config mean/median).

## Metrics reported per (query, config), warm 2–5
`tasks, avg batch, max batch, scan time, GPU decode, gpuTime, byte skew, wall ms`. The ftt comparison table always
shows **Δ wall, Δ scan time, Δ gpuTime** vs each fixed baseline (ftt − baseline). Data-read facts (on-disk listed,
read-off-disk `read_selectivity`, decoded `decode_expansion`, rows) come from the autotuner DECIDED log lines.
