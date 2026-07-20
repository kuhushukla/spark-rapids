# NDS query9 — Scan Split A/B

## Question
Does the scan split (`spark.sql.files.maxPartitionBytes`) actually change query time, once caching
and JIT are controlled for?

## Method (confounds controlled)
- Vary `spark.sql.files.maxPartitionBytes` ∈ {128m, 1024m, 4096m}. **Autotuner OFF** so the effective
  split == the value set (and is verified per-scan via the `scanMaxSplitBytes` event-log metric).
- **Page cache pre-warmed** (`cat` all store_sales+reason files → /dev/null) so every run reads from RAM.
- **Fresh JVM per split**; run query9 **6×** per JVM; report **steady state = median of iters 2–6**
  (iter 1 discarded as JIT/GPU warmup).
- A5000 (`GPU-1aaa66fd`), `local[16]`, plugin enabled, `metrics.level=DEBUG`.
- Harness: `handoff/run-ab-split.sh` + `handoff/aa-query9.scala`. Data: `ab-split-20260714_152702/`.

## Results — query9

| maxPartitionBytes | effective split (metric) | store_sales scan tasks | iter1 (warmup) | steady median (2–6) | speedup vs 128m |
|---|---|---|---|---|---|
| 128m  | 128 MiB  | 1232 | 27.8s | **25.7s** | 1.0× |
| 1024m | 1024 MiB |  157 |  8.2s | **5.35s** | **4.8×** |
| 4096m | 4096 MiB |   40 |  7.0s | **4.40s** | 5.8× |

## Grounding / cross-checks (three independent confirmations)
1. **Task counts** `1232 / 157 / 40` match a plain-Spark control probe on store_sales exactly
   (`128m→1232, 512m→313, 1024m→157, 4096m→40`) and the plugin-enabled probe.
2. **`scanMaxSplitBytes` metric** recorded the store_sales scan's effective split as
   `128 / 1024 / 4096 MiB` respectively (and `reason` as 4 MiB — its 2 KB size floors at
   `openCostInBytes`; unaffected by the knob). New metric added at
   `GpuFileSourceScanExec.scala` (driver metric next to `numPartitions`).
3. **Code path** confirmed: `StaticPartitionShims.getStaticPartitions` returns `None` for Spark 353
   (`spark330/.../StaticPartitionShims.scala:53`), so the override runs; `getFilePartitions`
   (`spark350/.../FilePartitionShims.scala:48`) does vanilla global bin-packing that responds to the
   split.

## Conclusion
**The split matters enormously: 128 MiB → 1 GiB gives ~4.8× on query9**, cleanly attributable to the
split (cache + JIT held constant). Mechanism: at 128 MiB store_sales is read by **1232 tiny tasks**
(~12 MB each, ~ms of GPU work); per-task scheduling/deserialization dominates. At 1 GiB it's **157
tasks**; at 4 GiB, **40**. Bigger splits ⇒ fewer tasks ⇒ far less overhead. Returns flatten past
1 GiB (1 GiB→4 GiB is only ~1.2×).

**Implication for the autotuner:** its core action — raising the split from the 128 MiB Spark default
to ~1 GiB for a table like store_sales — is worth ~4–5× on scan-heavy queries.

## Scope / caveats
- **query9 only.** query67 and query76 need the same clean harness before their split sensitivity
  can be stated.
- The autotuner's *chosen* split (vs. a manually set maxPartitionBytes) still needs its own A/B
  (enable `historyPath`, read `scanMaxSplitBytes` = the DECIDED value); the mechanism is proven, the
  specific decisions and their `parallelismCap`/`memCeiling` clamps are not re-measured here.
- Whether even-larger splits or the self-sizing ceiling help beyond ~1 GiB is a separate question;
  here the gain is flat past 1 GiB.
