# Counterfactual model: `spark.sql.files.maxPartitionBytes`

## Decision and evidence state

Candidate: choose file-partition limits that create enough useful scan work to feed the GPU without manufacturing empty/tiny tasks. This study evaluates one fixed Parquet snapshot; it does not propose a global default.

- **IMPLEMENTED:** Spark 3.5.5 consumes `spark.sql.files.maxPartitionBytes` when file partitions for a newly planned/materialized scan are computed.
- **IMPLEMENTED:** Spark byte-range partitions and RAPIDS GPU batches are different objects controlled by different limits.
- **MEASURED:** the NYC TLC snapshot has one row group and every tested layout retained exactly one useful scan task.
- **SUPPORTED mechanism:** limits below the row-group span created empty scan tasks.
- **EXPLORATORY performance:** seven blocks observed no median scan-stage improvement of at least 5%; the effect remains inconclusive.

The final protocol is retrospectively versioned, not claimed as an auditable preregistration.

## Configuration lifecycle

For selected files, Spark 3.5.5 computes:

```text
totalBytes = sum(file.length + openCostInBytes)
bytesPerCore = totalBytes / minPartitionNum
effectiveSplitBytes =
  min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))
```

It then creates byte ranges and packs them into `FilePartition` tasks. If `maxPartitionNum` is set and the initial count is too high, Spark may repack while ignoring `maxPartitionBytes`.

The SQL configuration is read when file partitions are planned/materialized. Changing it after a scan's partitions have been materialized does not resize those partitions. This experiment creates a fresh read/query after setting each value, pins `openCostInBytes=1` and `minPartitionNum=1`, and leaves `maxPartitionNum` unset.

## Baseline execution graph

```text
driver: list file -> footer/schema -> range and FilePartition planning
                                      |
                                      v
                     resource-constrained scan task schedule
             task i: footer/filter/read -> GPU queue -> GPU batch work
                                      |
                                shuffle barrier
                                      |
                         final GPU aggregate -> collect
```

Within a task, dependent phases are serial. Across tasks, CPU preparation, storage reads, and GPU work may overlap subject to eight CPU task slots, one GPU, admission, and shared resources. The shuffle is a barrier before the final aggregate.

The candidate changes only byte ranges and task count. It does not split a Parquet row group. RAPIDS independently groups selected Parquet blocks using row and estimated decoded-byte limits.

## Byte and work ledger

| Phase | Representation | Demand | Resource lane | Evidence |
|---|---|---:|---|---|
| range planning | file length plus modeled open cost | one file | driver CPU | Spark source |
| projected scan | encoded Parquet columns | task input bytes | local storage/CPU | event log |
| decode/filter | decoded projected columns | 6,405,008 rows total | GPU | event log/RAPIDS plan |
| partial aggregate | filtered rows and keys | one useful task | GPU | event log |
| shuffle/final aggregate | aggregate states | fixed logical result | local shuffle/GPU | event log |
| output | canonical aggregate rows | 1,144 rows | driver | preserved CPU payload |

The full file is 93,562,858 bytes. Its only row group contains 93,550,347 compressed column-chunk bytes and 170,542,631 uncompressed bytes. The projected scan reports a smaller input-byte count because it reads selected columns, so file size, projected encoded bytes, decoded bytes, and GPU-resident bytes must not be conflated.

## Resource-constrained schedule

For task `i`, use a task-local decomposition:

```text
task_ready_i =
  task_launch_i
  + footer_and_filter_i
  + projected_read_and_prepare_i

task_finish_i =
  resource_schedule(
    task_ready_i,
    gpu_queue_i,
    batches_i * (per_batch_setup_i + gpu_work_i),
    shared CPU/storage/GPU constraints
  )
```

Per-task/footer overhead belongs inside concurrently scheduled tasks. Per-batch setup belongs inside the GPU/batch schedule. Neither is a globally serial `P * H_task` or `K * H_batch` term unless a trace proves that serialization.

For validated capacity ceilings only:

```text
T_scan >= max(
  storage_demand / validated_storage_capacity_ceiling,
  CPU_demand / validated_CPU_capacity_ceiling,
  GPU_demand / validated_GPU_capacity_ceiling
)
```

Measured sustainable rates are calibrated typical-case estimates with uncertainty, not capacity ceilings and therefore not automatically lower bounds:

```text
typical lane time ~= lane demand / measured sustainable rate
```

The prediction must compose those estimates through the actual task/GPU schedule, including fill, drain, queueing, and tail imbalance.

## Break-even boundary

For a smaller limit `M2 < M1`:

```text
saved starvation or tail time
  >
additional resource-scheduled task, footer, queue, and batch work
```

A necessary condition for a parallelism win is an increase in independently useful work. For Parquet that is bounded by row-group placement, not byte-range count alone.

This file has one row group. Therefore:

```text
useful_tasks(M) = 1
empty_tasks(M) = planned_tasks(M) - 1
```

for every tested `M`. The necessary parallelism condition is false before timing is considered.

## Measured split verdict

| MiB | Planned / useful / empty scan tasks | Median scan stage |
|---:|---:|---:|
| 128 | 1 / 1 / 0 | 193 ms |
| 64 | 2 / 1 / 1 | 190 ms |
| 32 | 3 / 1 / 2 | 193 ms |
| 16 | 6 / 1 / 5 | 193 ms |
| 8 | 12 / 1 / 11 | 191 ms |

- Empty-task mechanism: **SUPPORTED**.
- Performance effect: **EXPLORATORY / INCONCLUSIVE**.
- Bounded observation: no candidate's observed median improved scan-stage makespan by at least 5% versus 128 MiB in seven blocks.
- The paired bootstrap intervals are exploratory and unadjusted across four comparisons. They are not confirmatory noninferiority, equivalence, or superiority evidence.

## Risks and validity

- One row group cannot reveal the point where useful task parallelism begins feeding the GPU.
- Warm local storage does not model object-store latency or distributed bandwidth.
- A single projection/filter cannot validate sensitivity to projected width or selectivity.
- File partitions and GPU batches remain distinct distributions.
- Seven blocks are useful for mechanism observation and variance reconnaissance, not a confirmatory performance claim.

## Priority decision

**MEASURE.** Avoid lowering the limit below the only row-group span for this snapshot because doing so creates provably empty tasks. Do not claim a latency benefit or a generally optimal value.

The next experiment needs multiple files with at least 8–16 independently useful, uneven row groups, plus a hold-out projection/selectivity. A performance claim would require an independently versioned confirmatory design with multiplicity and noninferiority/equivalence rules chosen before results.
