# Composable GPU scan performance model and plugin-local history POC

Status: proof of concept. This is an internal model and storage experiment, not a
user-facing configuration or production autotuner.

## Decision and knobs

For each GPU table read, use the information available at that scan's partition-planning
boundary to choose a partition/batch region that supplies enough decoded rows and bytes
to amortize fixed costs and remains below the resident-memory wall. When reliable live
resource information is available, also retain enough tasks to overlap work. The output
is a bounded region with evidence and uncertainty, not one universal "optimal byte"
value.

The product decision belongs inside the RAPIDS plugin, independently for every scan.
A benchmark launcher can create evidence, but it cannot make the runtime decision for
an application that reads several tables with different schemas, predicates, and data
shapes.

`spark.sql.files.maxPartitionBytes` controls encoded input assigned to a task. It
changes task count, waves, file/row-group splitting, and overhead amortization.
`spark.rapids.sql.batchSizeBytes` targets decoded GPU batch size. A large partition can
produce several batches; a small partition cannot produce a batch larger than its
decoded output. The chunked reader and soft limit further mediate actual batch sizes.

The policy must predict both encoded partition -> decoded bytes/rows, for fill and
memory, and decoded batch -> footprint/component service, for safety and throughput.

## Evidence identity is not prediction matching

Every observation retains exact provenance so it can be audited: query/plan, table
snapshot, schemas, literal predicates, software, hardware, storage, reader, configuration,
and observed resources. That identity answers "what produced this measurement?" It must
not be used as an all-or-nothing lookup key.

Prediction uses a small set of component-specific features. Each component consumes only
the features that affect its mechanism:

| Component | Primary reusable features | Optional refinements |
|---|---|---|
| encoded -> decoded bytes/rows | format, codec when known, projected column types, nullability, schema-evolution state | table/column history, file statistics, predicate selectivity |
| decoded -> resident footprint | decoded bytes/rows, types, reader/batch path, RAPIDS/cuDF compatibility epoch | GPU memory architecture and operator temporaries |
| decode/filter rate | format, projected types, predicate/operator class, RAPIDS/cuDF epoch | GPU class, batch rows/bytes |
| read service | filesystem/connector/client mode and request policy | recent live throughput, latency, throttling, cache state |
| fixed task cost | Spark/RAPIDS execution path and reader mode | JVM/runtime epoch |
| task-wave penalty | predicted task sizes/count, skew/layout | current executor slots and admission capacity |
| downstream operator work | operator class plus input rows/bytes/types | the plan fragment available inside the current AQE boundary |

Table, snapshot, query, and full-plan fingerprints may improve a prediction but are never
universal prerequisites. A date literal changing from one day to the next is a feature
value, not a new model family. A newly added column uses compatible per-column/type priors
and a wider interval rather than forcing complete cold start.

Selection follows a fallback lattice with partial pooling:

1. compatible table/column history for the requested projection and predicate class;
2. compatible history from the same table across snapshots or related predicates;
3. compatible column/type, format/codec, reader, and operator observations from other
   tables and queries;
4. conservative generic priors;
5. the user's normal Spark split behavior when uncertainty makes adaptation unsafe.

More specific evidence shrinks the generic estimate; it does not replace it with a brittle
exact match. Missing optional features widen uncertainty or disable only the dependent
component. For example, missing executor-count information disables a precise wave-cost
prediction but does not prevent decoded-size or memory-safety estimation.

Hard compatibility boundaries are narrow: incompatible metric meaning, file/reader
semantics, or software changes known to alter a component's mechanism. Ordinary query
text, date ranges, snapshots, schema additions, executor-count changes, and transient
storage rates are observations or features, not hard invalidations.

## Per-read inputs and decision boundary

For V1 non-bucketed file scans, the plugin decision point is
`GpuFileSourceScanExec.createNonBucketedReadRDD`. It runs after dynamic partition pruning
and immediately before `splitFiles` and `getFilePartitions`. The POC should replace the
single call to Spark's `FilePartition.maxSplitBytes` with a high-level planner API that
can return either an adapted limit or `UseSparkDefault`.

Inputs already available at that point are:

- required read schema and relation/data/partition schemas;
- selected partition directories, file paths, encoded lengths, and partition values;
- partition and pushed data filters, including resolved dynamic partition pruning;
- file format and table identifier when Spark supplies one;
- SQLConf/RAPIDS reader, batching, and partition-planning settings; and
- the scan operator plus the plan fragment already materialized in the current AQE
  boundary when the caller chooses to provide it.

Optional live inputs include current executor/slot estimates, GPU admission/memory state,
and recently observed storage service. They are timestamped hints, not stable identity.
The number of executors may change after planning; the policy must attach uncertainty and
must not make memory safety depend on that number.

Information outside the current AQE boundary is optional. The planner must remain useful
with only scan-local inputs. Bucketed reads, DataSource V2, Hive, Delta, and Iceberg have
different partition-planning hooks and remain explicit follow-on integrations rather
than being hidden behind an inaccurate claim of universal coverage.

### Minimum viable split decision

The first useful policy does not require shuffle, whole-query timing, storage bandwidth,
or a stable executor count. It requires:

1. Spark's split limit as the baseline and the selected file lengths/layout;
2. read/data schemas, projection, pushed predicates, format, and codec when known;
3. predicted decoded rows and bytes per candidate, with error bounds;
4. an upper task-footprint estimate and a safe per-task/admission budget when increasing
   above Spark's default; and
5. empirical lower efficiency bounds in decoded rows and bytes.

The minimum historical quantities are therefore data-description and safety parameters:
rows per encoded byte, decoded width by compatible column/type, footprint amplification,
and residual quantiles. A generic efficiency prior may seed the decoded-row/byte floor.
Read/decode/operator throughput and current slots improve performance ranking but are
optional refinements.

Hardware is component-specific. Data expansion should transfer without CPU/GPU identity.
A footprint or decode-rate component may need a RAPIDS/cuDF compatibility epoch and GPU
memory/class. If a trustworthy GPU memory/admission budget is unavailable at the driver,
the POC must not increase the memory-risking upper bound; it can still avoid excessively
small splits or return Spark's default. If executor count is unavailable or unstable,
omit precise wave optimization and use a conservative task-count floor.

## Quantities and metric semantics

For observation `i`:

- `D_i`: compressed bytes actually read, excluding metadata-pruned ranges;
- `U_i, R_i`: decoded bytes and rows before the SQL filter;
- `SBytes_i, SRows_i`: surviving bytes and rows;
- `N_i, B_i`: useful tasks and output batches;
- `Wr_i, Wd_i, Wf_i, Wx_i`: summed read, decode, filter, and downstream service;
- `Cr_i, Cg_i`: independently measured read and GPU capacities;
- `M_i`: maximum task footprint;
- `Ts_i, Tq_i`: scan-stage and query wall time.

These are not all additive wall times:

- RAPIDS scan time wraps iterator progress and includes overlapping work.
- GPU decode time is host wall around `Table.readParquet`, not CUDA kernel time.
- filesystem read time measures filesystem calls, not all scheduling/buffering.
- scan-internal filter time is footer/row-group pruning, not SQL `GpuFilter`.
- async buffer, scheduling, and read metrics overlap and must not be summed blindly.
- CUDA kernel service sums kernel durations; CUDA busy time unions kernel intervals.

The experiment extracts DEBUG scan, buffer, internal-filter, I/O-schedule, bubble,
filesystem-read, buffer-write, GPU-decode, SQL-filter, semaphore, footprint, retry, and
spill metrics where available.

## Decoded-size and row prediction

Historical observations and currently available metadata predict a proposed encoded
partition:

```text
predictedDecodedRows =
  encodedBytes * rowsPerEncodedByte(dataFeatures)

predictedDecodedBytes =
  sum(predictedDecodedRows * decodedWidth(columnFeatures))
  + variableWidthAndValidityOverhead
```

A direct robust decoded-bytes/encoded-byte estimate remains a useful table-specific
measurement and cross-check. The decomposed row/column model is the transfer path when the
query, projection, snapshot, or schema changes. `dataFeatures` includes only available
format/codec, file statistics, partition/predicate selectivity, and compatible table
history. Each `columnFeatures` entry describes logical/physical type, nullability,
projection, legal up-cast, variable-width evidence, and whether older files omit the
column.

A new or missing column therefore contributes a type-based prior and larger uncertainty;
known columns retain their learned estimates. Predicate literals are normalized into
column/operator/value features. When file or catalog statistics can estimate selectivity,
use them; otherwise borrow a compatible historical selectivity distribution and widen
the interval.

Use robust hierarchical estimates and retain residual quantiles at each fallback level.
Safety uses upper bounds:

```text
safeDecodedBytes =
  upperQuantile(predictedDecodedBytes | availableDataShapeFeatures)

safeFootprint =
  upperQuantile(predictedFootprint | safeDecodedBytes, rows, availableFootprintFeatures)
```

Rows and bytes are both required. Bytes bound memory; rows often explain per-row work.

## Component and read models

The simplest deployable model uses independently reusable component rates. Each rate is
units divided by the corresponding summed call/service time, before overlap:

```text
taskFixedService  = launchedTasks * fittedFixedCostPerTask(taskFeatures)
readService       = encodedBytes / effectiveReadRate(readFeatures, recentLiveReadState)
decodeService     = decodedBytes / effectiveDecodeRate(decodeFeatures)
filterService     = decodedRows / effectiveFilterRate(filterOperatorFeatures)
downstreamService = sum(operatorWork_j / effectiveRate(operatorFeatures_j))
```

There is no single global `K`. Each estimator retrieves compatible observations using
its own features and fallback lattice. Data expansion can transfer across hardware;
decode rate may depend on GPU and RAPIDS/cuDF epoch; read rate depends on the client and
recent storage state; operator rates depend on operator class and input shape rather
than a whole-query fingerprint.

Downstream work has an explicit operator fingerprint and unit; bytes and rows are never
mixed dimensionally. Local warm-data read service rate includes page-cache behavior.
The selected object-store client, coalescing, and throttle policy remain in the key, but
request overlap is represented separately by `effectiveReadCapacity`; concurrency is
not counted in both quantities.

A detailed read submodel is needed only when changing range coalescing/client design:

```text
readService ~= requestCount * requestLatency + transferredBytes / linkBandwidth
```

One blocking client is serial. Multiple blocking threads use a bounded parallel
schedule. An async client uses an outstanding-request schedule. This remains behind the
same API and cannot reuse rates from a different client context.

## Overlap-aware wall model

An additive sum is service accounting, not stage wall: tasks pipeline read, decode, and
GPU work. The deployable lower bound is:

```text
L = max(
  taskFixedService / effectiveTaskCapacity,
  readService / effectiveReadCapacity,
  (decodeService + filterService + downstreamService) / measuredGpuOverlapCapacity,
  predictedLongestUsefulTask
)
```

The fixed task cost must be fitted externally from task-count ramps or explicit task
setup metrics; it
is not hidden in a size-bucket residual. Empty launched tasks remain in
`launchedTasks`, even when `usefulTasks` is zero for their output.

Read capacity is not inferred from GPU holders. GPU overlap capacity is predicted from
the union of the same host service intervals whose sums appear in the numerator (or by
a task list scheduler); CUDA kernel-service/kernel-busy overlap cannot divide host decode
wall. Holder count is only a feasibility cap. The candidate longest task is supplied
from the predicted task assignment/skew model. The mathematical floor is variable
service divided by useful tasks plus fixed service divided by launched tasks, so empty
task setup is not assigned to useful tasks. Candidate
GPU capacity is capped by useful tasks and by
`admissionBudgetBytes / maxTaskFootprintBytes`.

The exploratory calibrated estimate is:

```text
predictedStageWall = L * calibratedResidualScale(availableFeatures, sizeBucket)
```

Residual p10/p90 scales report uncertainty. Query wall adds a separately measured
non-scan tail. Production should replace the scalar residual with a bounded list
scheduler when task-level timelines are available.

This represents parallel work as parallel and serial work as serial. Small partitions
inflate task/decode launch work and produce partial batches. Large partitions collapse
useful tasks/waves; the longest task dominates, admission may fall, and spill/retry adds
cliffs.

## Measured local evidence

The preregistered Nsight Systems runs use `local-rtx-a6000`, Spark 3.5.5
`local[8]`, warm local Snappy Parquet, the 2009 taxi common-column aggregate, 1-GiB
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

## Fresh query-shape transfer evidence

A separate 48-run, three-block trial added multi-key aggregate, self-join, window/top-N,
and wide selective-filter shapes. Its raw evidence is internally reproducible, but its
preregistration file has no immutable pre-execution binding and is therefore treated as
descriptive.

Median curves split by downstream work: aggregate/filter shapes favored larger
partitions, while self-join/window shapes favored smaller partitions. Scan tasks changed
from 212 to 5 for the full-corpus queries and from 39 to 1 for the window query. No
single tested size was within 10% of best on every shape. The restricted minimax was
512 MiB with 12.50% worst point regret.

This supports using downstream operator components when they are visible, and rejects the
tested global candidates as universally within 10%. It does not support an exact
whole-query fingerprint requirement. AQE was off; an AQE-on paired shuffle-heavy
follow-up is required to learn how much downstream prediction can safely transfer when
later stages are not yet planned.
The trial is documented under `docs/experiments/fresh-query-holdout/`.

## Bounded selection rule

For each candidate, always predict decoded bytes, rows, batches, and an upper footprint
bound. Predict useful tasks, component service, admitted capacity, and task-wave cost only
when their required inputs are available with adequate confidence. Retain candidates
satisfying:

- decoded bytes/rows are above the empirically efficient fill region;
- upper footprint prediction fits the admission-friendly budget;
- no retry, split, or spill is predicted; and
- every enabled optional component satisfies its declared uncertainty/regret bound.

When a fresh estimate of runnable slots is available, prefer enough useful tasks for the
configured wave margin. When it is absent or unstable, do not reject the entire model:
omit the precise wave objective, cap how far the policy moves toward very large splits,
and retain a conservative multi-task floor derived from Spark's current default and
selected-file layout.

Choose the largest conservative candidate inside the supported box because the upper wall
has sharper failure modes. If uncertainty is high, shrink the recommendation toward
Spark's calculated default. If no candidate is demonstrably safer or better, return
`UseSparkDefault`.

## Prototype API and limits

The committed `PerformanceHistory` is evidence that local recording and component-rate
calculation are feasible, but its exact 29-field `PerformanceContext` equality lookup is
the wrong retrieval design for the intended product. It must not be integrated into scan
planning unchanged.

The replacement high-level boundary is:

```text
planScan(ScanPlanningRequest) ->
  UseAdaptiveSplit(bytes, confidence, reasons)
  | UseSparkDefault(reasons)
```

`ScanPlanningRequest` contains required scan-local inputs and optional plan/resource
features. Internally, independent estimators return typed predictions:

```text
DataShapePrediction(rows, decodedBytes, interval, evidenceLevels)
FootprintPrediction(residentBytes, upperBound, evidenceLevels)
EfficiencyRegion(minRows, minDecodedBytes, confidence)
Optional[ServicePrediction]
Optional[WavePrediction]
```

The selector composes these predictions; it does not ask one monolithic history lookup to
recognize the query. Each observation retains exact provenance, while each estimator
declares the subset of features it consumes, its fallback level, sample weight/age, and
uncertainty. Tests must cover changed date literals, added/removed columns, legal up-casts,
null-filled missing columns, related tables, missing table IDs, missing hardware/live
resources, and software compatibility epochs.

Persistence remains hidden behind the API. The current line protocol demonstrates
truncation recovery but is driver-owned, synchronous, single-instance, and unbounded. It
must not be called on the measured critical path. Production needs buffered asynchronous
writes, record IDs, bounded retention/atomic compaction, multi-process coherence,
permissions, and schema-versioned component observations.

No public Spark/RAPIDS configuration has yet been added. The eventual opt-in configuration
controls whether the per-scan planner may replace Spark's calculated split limit; it does
not provide one application-wide replacement value.

## Implementation path

1. Replace the monolithic exact context with exact observation provenance plus separate
   component feature schemas and fallback policies.
2. Implement `GpuScanPartitionPlanner` as a separate internal package. First integrate
   only the V1 non-bucketed hook immediately before `splitFiles`; all unsupported paths
   return Spark's existing `maxSplitBytes`.
3. Implement the decoded-row/byte and upper-footprint estimators first. They require only
   scan-local inputs and directly protect GPU fill and memory.
4. Add optional live resource and visible-plan operator components without making them
   prerequisites for the first two estimators.
5. Construct observations from scan SQL metrics plus exact provenance. Record after
   successful work; tag retry, spill, fallback, and censored observations instead of
   treating them as ordinary throughput.
6. Run the planner in shadow mode per scan and log Spark default, candidate, component
   evidence levels, uncertainty, and eventual scan outcomes.
7. Validate transfer explicitly across changed date predicates, projections, schema
   evolution, related tables, snapshots, and executor counts. Promote only after
   prospective coverage and bounded-regret/safety criteria pass.
8. Add V2, Hive, Delta, Iceberg, and bucketed planning hooks one at a time with shim and
   integration coverage; do not route them through the V1 implementation by assumption.

Raw profiles, preregistration, analyzers, and checksums are under
`docs/experiments/profiled-gpu-tuning-poc/`.
