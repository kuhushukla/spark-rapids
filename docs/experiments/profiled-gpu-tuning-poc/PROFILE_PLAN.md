# Profiled GPU tuning proof of concept

Status: **PREREGISTERED BEFORE EXECUTION; NSYS, CUPTI, AND JFR EXECUTED**

## Objective

Validate whether context-keyed historical service measurements can predict a robust
GPU scan batch/partition region when I/O, decode, filtering, downstream work, and task
overlap are modeled separately.

The proof of concept is not a production autotuner. It must:

1. preserve effective read, decode, filter, and downstream service measurements;
2. key observations by hardware/instance, table/snapshot, projection, predicate,
   storage/cache domain, and reader/client behavior;
3. distinguish summed service demand from stage wall time;
4. model task/batch overlap and useful-task wave collapse;
5. return a bounded prediction with evidence and uncertainty, not a single unexplained
   setting.

## Runtime context

- Instance key: `local-rtx-a6000` (not transferable to a cloud instance label).
- GPU: NVIDIA RTX A6000, 49,140 MiB.
- Spark: 3.5.5, local[8].
- Data: sharded 2009 taxi table, 347 one-row-group Snappy Parquet files.
- Projection/query: common two-column full scan plus partial/final aggregate.
- Storage/cache context: local filesystem, repeated/warm-session access; OS page-cache
  state is not directly observable.
- Reader: current RAPIDS reader selected by the committed runtime configuration.
- Dynamic GPU admission: enabled, initial concurrency four.
- RAPIDS batch target: 1 GiB; reader soft limit: 2 GiB.

## Profiles

### JFR plus device sampling

Run one warmup followed by three randomized blocks at 128, 2,048, and 16,384 MiB.
JFR captures driver/executor CPU activity in the local-mode JVM. A 100-ms
`nvidia-smi` sample captures coarse device utilization, memory, power, and clocks.
Profiled wall times are diagnostic and are not mixed with unprofiled benchmark timing.

### Stage-gated RAPIDS CUPTI profile

For each of 128, 2,048, and 16,384 MiB, run the frozen sequence: common warmup,
variable-width warmup, then the common target. Profile only scan stage 4 with the
built-in RAPIDS CUPTI profiler. Convert its binary output to JSON and retain:

- summed CUDA kernel duration as GPU service demand;
- the union of kernel intervals as GPU busy time;
- memcpy duration and bytes;
- NVTX `Parquet decode` and task/GPU-ownership ranges;
- idle gaps and overlap on the stage critical path.

The stage ID is valid only for the exact frozen three-run schedule and must be verified
from every event log.

### Nsight Compute

Run one isolated common-query cell at each of 128, 2,048, and 16,384 MiB.
Collect the Basic section for the first 40 kernel launches enclosed by the existing
`Parquet decode` NVTX range. Nsight Compute replay perturbs execution; only kernel
characteristics are used, never its query wall time.

Nsight Systems 2025.2.1 is bundled outside `PATH` under the installed Nsight Compute
tree. Run the same frozen three-run schedules with CUDA, NVTX, OS-runtime, and 1-kHz GPU
metric tracing. Use the event log and task NVTX ranges to restrict analysis to target scan
stage 4. CUPTI and Nsight Systems traces are independent cross-checks; neither profiled
wall time is used as an unprofiled performance observation.

## Prediction validation

Training source: 2009 dynamic-concurrency mechanism sweep.

Prospective checks:

- 2010 and 2011 dynamic reruns already versioned in the parent experiment;
- three-year/table-growth evidence only as an additional exploratory check because its
  static-concurrency protocol differs.

Compare:

1. additive component model;
2. bottleneck/max lower-bound model;
3. overlap-aware task/batch scheduler.

Acceptance targets for a later confirmatory run:

- median stage-time error no worse than 10%;
- p90 error no worse than 15%;
- correct identification of the bottleneck family;
- correct avoidance of small-batch and useful-task-collapse cells;
- prediction intervals with observed coverage near their nominal level.

The current five-block data cannot establish exact two-sided p < 0.05 or ±5%
equivalence. Any validation here is exploratory and must be labeled accordingly.
