# Context-keyed GPU scan performance model and local history POC

Status: proof of concept. This is an internal model and storage experiment, not a
user-facing configuration or production autotuner.

## Decision and knobs

For a known hardware, table, projection, predicate, storage, and reader context, choose a
scan partition/batch region that supplies enough decoded rows and bytes to amortize
fixed costs, retains enough tasks to overlap work, and remains below the resident-memory
wall with prediction-error margin. The output is a bounded region with evidence and
uncertainty, not one universal "optimal byte" value.

\`spark.sql.files.maxPartitionBytes\` controls encoded input assigned to a task. It
changes task count, waves, file/row-group splitting, and overhead amortization.
\`spark.rapids.sql.batchSizeBytes\` targets decoded GPU batch size. A large partition can
produce several batches; a small partition cannot produce a batch larger than its
decoded output. The chunked reader and soft limit further mediate actual batch sizes.

The policy must predict both encoded partition -> decoded bytes/rows, for fill and
memory, and decoded batch -> footprint/component service, for safety and throughput.

## Reuse key

Measurements are reusable only inside a declared context. The POC key contains:

- instance or local hardware label, GPU model/memory, and CPU cores;
- Spark, RAPIDS, Java, connector versions, and a versioned execution fingerprint that
  covers executor/GPU topology, batch/read limits, reader path, and admission policy;
- table and snapshot, schema, format, and codec;
- canonical projection, partition predicate, data predicate, and downstream-stage
  fingerprints;
- filesystem, storage, and cache context; and
- client kind, blocking/asynchronous behavior, reader threads, outstanding-request
  limit, read-ahead, range-merge gap, and throttle policy.

A serial blocking client is not interchangeable with an asynchronous client. The query
key uses canonical expressions, not SQL text. Predicates with materially different
selectivity need separate keys or selectivity features. Raw table/storage identifiers
should be hashed before a production implementation persists them.

## Quantities and metric semantics

For observation \`i\`:

- \`D_i\`: compressed bytes actually read, excluding metadata-pruned ranges;
- \`U_i, R_i\`: decoded bytes and rows before the SQL filter;
- \`SBytes_i, SRows_i\`: surviving bytes and rows;
- \`N_i, B_i\`: useful tasks and output batches;
- \`Wr_i, Wd_i, Wf_i, Wx_i\`: summed read, decode, filter, and downstream service;
- \`Cr_i, Cg_i\`: independently measured read and GPU capacities;
- \`M_i\`: maximum task footprint;
- \`Ts_i, Tq_i\`: scan-stage and query wall time.

These are not all additive wall times:

- RAPIDS scan time wraps iterator progress and includes overlapping work.
- GPU decode time is host wall around \`Table.readParquet\`, not CUDA kernel time.
- filesystem read time measures filesystem calls, not all scheduling/buffering.
- scan-internal filter time is footer/row-group pruning, not SQL \`GpuFilter\`.
- async buffer, scheduling, and read metrics overlap and must not be summed blindly.
- CUDA kernel service sums kernel durations; CUDA busy time unions kernel intervals.

The experiment extracts DEBUG scan, buffer, internal-filter, I/O-schedule, bubble,
filesystem-read, buffer-write, GPU-decode, SQL-filter, semaphore, footprint, retry, and
spill metrics where available.

## Decoded-size and row prediction

Historical table/file statistics predict a proposed encoded partition:

\`\`\`text
predictedDecodedBytes = encodedBytes * historicalDecodedBytesPerEncodedByte(K)
predictedDecodedRows  = encodedBytes * historicalDecodedRowsPerEncodedByte(K)
\`\`\`

Projection and predicate are in \`K\`, because expansion, width, and selectivity change
the ratios. Schema evolution is represented explicitly for common columns, legal
Parquet up-casts, and null-filled missing columns.

Use robust ratios/regressions and retain residual quantiles. Safety uses upper bounds:

\`\`\`text
safeDecodedBytes = upperQuantile(predictedDecodedBytes | K)
safeFootprint = upperQuantile(predictedFootprint | safeDecodedBytes, rows, K)
\`\`\`

Rows and bytes are both required. Bytes bound memory; rows often explain per-row work.

## Component and read models

The simplest deployable model uses context-conditioned per-service-demand rates. Each
rate is units divided by the corresponding summed call/service time, before overlap:

\`\`\`text
readService       = encodedBytes / effectiveReadRate(K)
decodeService     = decodedBytes / effectiveDecodeRate(K)
filterService     = decodedRows / effectiveFilterRate(K)
downstreamService = downstreamWorkUnits / effectiveDownstreamRate(K)
\`\`\`

Downstream work has an explicit operator fingerprint and unit; bytes and rows are never
mixed dimensionally. Local warm-data read service rate includes page-cache behavior.
The selected object-store client, coalescing, and throttle policy remain in the key, but
request overlap is represented separately by `effectiveReadCapacity`; concurrency is
not counted in both quantities.

A detailed read submodel is needed only when changing range coalescing/client design:

\`\`\`text
readService ~= requestCount * requestLatency + transferredBytes / linkBandwidth
\`\`\`

One blocking client is serial. Multiple blocking threads use a bounded parallel
schedule. An async client uses an outstanding-request schedule. This remains behind the
same API and cannot reuse rates from a different client context.

## Overlap-aware wall model

An additive sum is service accounting, not stage wall: tasks pipeline read, decode, and
GPU work. The deployable lower bound is:

\`\`\`text
L = max(
  readService / effectiveReadCapacity,
  (decodeService + filterService + downstreamService) / measuredGpuOverlapCapacity,
  longestUsefulTask
)
\`\`\`

Read capacity is not inferred from GPU holders. GPU overlap capacity is predicted from
the union of the same host service intervals whose sums appear in the numerator (or by
a task list scheduler); CUDA kernel-service/kernel-busy overlap cannot divide host decode
wall. Holder count is only a feasibility cap. The
longest task is measured or
predicted from task-level history, not total service divided by task count. Candidate
GPU capacity is capped by useful tasks and by
\`admissionBudgetBytes / maxTaskFootprintBytes\`.

The exploratory calibrated estimate is:

\`\`\`text
predictedStageWall = L * median(observedStageWall / historicalL | K, sizeBucket)
\`\`\`

Residual p10/p90 scales report uncertainty. Query wall adds a separately measured
non-scan tail. Production should replace the scalar residual with a bounded list
scheduler when task-level timelines are available.

This represents parallel work as parallel and serial work as serial. Small partitions
inflate task/decode launch work and produce partial batches. Large partitions collapse
useful tasks/waves; the longest task dominates, admission may fall, and spill/retry adds
cliffs.

## Measured local evidence

The preregistered Nsight Systems runs use \`local-rtx-a6000\`, Spark 3.5.5
\`local[8]\`, warm local Snappy Parquet, the 2009 taxi common-column aggregate, 1-GiB
RAPIDS batch target, and dynamic admission.

| maxPartitionBytes | tasks | Spark stage wall | GPU ownership envelope | kernel service | kernel busy | max simultaneous kernels |
|---:|---:|---:|---:|---:|---:|---:|
| 128 MiB | 87 | 569 ms | 557.1 ms | 124.9 ms | 112.5 ms | 6 |
| 2,048 MiB | 5 | 279 ms | 266.3 ms | 34.3 ms | 31.3 ms | 4 |
| 16,384 MiB | 1 | 469 ms | 456.3 ms | 30.1 ms | 30.1 ms | 1 |

The trace reports about 1.68 GiB of aggregate CUDA memcpy bytes in every cell; this is
not unique input bytes or a single transfer direction. The small cell launches 10,962 kernels
versus 735 at 2 GiB and has four times the decode NVTX union, supporting the fixed-work
wall. At the large end, kernel service stays near 30 ms while Spark stage wall grows 68% and
simultaneous kernels fall from four to one. That supports lost task-level pipeline opportunity and longer non-GPU idle gaps, not
increased GPU computation; the roughly 3-ms kernel-overlap change alone cannot explain
the approximately 190-ms wall increase. Mean SM-active is 7.1% at 16 GiB versus 12.0% at 2 GiB;
both peak at 100%, so the large cell contains longer idle gaps.

Unprofiled DEBUG metrics independently put summed filesystem-read time near 32--42 ms
from 2--32 GiB and GPU-decode host wall near 264 ms at 2 GiB versus 86 ms at 16--32
GiB. This warm-cache result does not transfer to S3.

These profiles validate mechanisms, not a 10%-accurate predictor: only three cells were
profiled and profiler wall times are perturbed. The preregistered 10% median and 15% p90
error targets require prospective repeated holdouts.

## Bounded selection rule

For each candidate, predict decoded bytes, rows, useful tasks, component service,
admitted capacity, and footprint. Retain candidates satisfying:

- decoded bytes/rows are above the empirically efficient fill region;
- at least three to four useful task waves exist when layout permits;
- upper footprint prediction fits the admission-friendly budget;
- no retry, split, or spill is predicted; and
- worst-case regret under uncertainty is within tolerance.

Choose the largest retained conservative candidate below the high-end cliff. If the
default is already inside the box, do nothing.

## Prototype API and limits

\`PerformanceHistory.local(path)\` returns an internal API that records observations and
predicts from an exact key plus nearby typical-batch-size bucket. It estimates component
service rates, applies separately supplied empirical read/GPU overlap capacities and a
longest-task bound, floors footprint-limited admission slots, and reports memory status.
It refuses unsupported size extrapolation and suppresses residual intervals below five
samples.

Persistence is a versioned line protocol hidden behind the API. It recovers by ignoring
an incomplete final record and fails on earlier corruption. This POC is deliberately
driver-owned and single-instance, appends synchronously after a completed measured
stage, and has unbounded retention. It must not be called on the measured critical path.
It accepts externally predicted decoded bytes/footprint; the upper-quantile safety model
and complete task/file-layout overhead model are not yet implemented. Its
`sumSqlFilterNs` input means SQL `GpuFilter` service only; scan footer/row-group pruning
is not modeled yet and must not be supplied under that name. Production needs
one buffered writer, record IDs, bounded retention/atomic compaction,
multi-process coherence, file permissions, lifecycle integration, and hierarchical
cross-snapshot matching.

No public Spark/RAPIDS configuration was added and no GPU operator was modified.

## Implementation path

1. Construct observations from SQL metrics plus canonical plan/table metadata.
2. Add explicit decoded/survivor bytes, effective read capacity, longest-task service,
   and kernel-busy metrics where accumulators are insufficient.
3. Record after successful stages; tag retry/spill instead of treating them as ordinary
   throughput.
4. Run predictions in shadow mode and compare intervals prospectively.
5. Promote only after holdout coverage and bounded-regret criteria pass.

Raw profiles, preregistration, analyzers, and checksums are under
\`docs/experiments/profiled-gpu-tuning-poc/\`.
