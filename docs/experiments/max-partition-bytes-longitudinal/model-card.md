# Counterfactual model card: longitudinal and schema-aware partition planning

## Status

**STAGE 1 NOT EXECUTED**

The immutable Stage 1 snapshot is `preregistration/preregistration.json`. It is
generated only after this specification is final and must verify before execution.

## Decision contract

- Decision boundary: before a new file scan is planned.
- Inputs: frozen metadata, query/read-schema identity, effective split inputs, and
  predicted physical work.
- Action: `SELECT(candidate)` only when one metadata-dominance survivor exists;
  otherwise `ABSTAIN(fallback=128 MiB)`.
- Objective: mechanically validate schema-aware planned/useful/empty work and the exact
  metadata-dominance filter.
- Performance contract: raw timings and paired descriptive dominance differences only.
- Authority: local GPU experiment/versioning on
  `experiment/max-partition-bytes-longitudinal`; no production or external change.
- Budgets: whole wrapper at most 1,800 seconds; GPU phase at most 1,230 seconds and
  0.35 GPU-hours.

## Evidence state

- **MEASURED:** 36 files, 13,580,215,763 bytes, 516,784,476 rows, six schemas, and one
  row group per file.
- **MEASURED:** exact census completed in 917.145066 ms.
- **MEASURED:** all 36 episode/candidate comparisons matched both Spark's CPU
  `FileSourceScanExec` and the pinned RAPIDS `GpuFileSourceScanExec` for planned
  tasks/ranges, useful tasks, empty tasks/ranges, physical-layout hash, and
  useful-layout hash.
- **MEASURED:** Spark automatic merge failed for observed `INT/DOUBLE` and
  `STRING/BIGINT` conflicts.
- **IMPLEMENTED:** minimal explicit read schemas, per-run physical-layout
  revalidation, metadata dominance, sparse retry/spill validation, prereg freeze and
  verification, Stage 1 validation/replay, and wrapper validation.
- **UNMODELED:** bytes materialized for missing nullable `PULocationID`.
- **PROPOSAL:** any performance ranking or production tuning policy.

## Execution graph

```text
frozen identity/metadata -> explicit read schema -> effective file/range planning
                                     |
                                     v
resource-constrained parallel scan tasks
 task: read -> GPU queue -> GPU batches/filter/partial aggregate
                                     |
                               shuffle barrier
                                     |
                           final aggregate -> collect
```

Dependent work within a task is serial. Tasks execute in a resource-constrained
parallel schedule. Stage elapsed time is not a sum of task durations.

## Frozen decision-time features

The selector uses:

- file count, encoded bytes, row-group rows and bytes;
- selected-column chunk metadata;
- schema fingerprints, presence/type features, and evolution conflicts;
- normalized query and minimal explicit read-schema identities;
- configured and effective split inputs;
- planned tasks/ranges, useful tasks, and empty tasks/ranges;
- physical- and useful-layout hashes;
- pinned software and topology identity.

Current-run event metrics and timings do not affect the selector.

Schema evolution is a first-class feature. For the evolution query, absent
`PULocationID` values are counted as missing row values and materialized as nullable
values by the explicit schema. Their materialized byte demand remains
`UNMODELED`.

## Exact metadata-dominance filter

For candidate A and candidate B in the same episode, A is dominated by B exactly when:

1. `useful_layout_sha256[A] == useful_layout_sha256[B]`;
2. `planned_tasks[A] >= planned_tasks[B]`;
3. `empty_tasks[A] >= empty_tasks[B]`;
4. `planned_ranges[A] >= planned_ranges[B]`;
5. `empty_ranges[A] >= empty_ranges[B]`; and
6. at least one comparison in 2–5 is strict.

`physical_layout_sha256` equality separately identifies fully equivalent physical
treatments.

After removing dominated candidates:

- one survivor produces `SELECT`;
- zero or multiple survivors produce `ABSTAIN` with 128 MiB fallback.

This is a structural partial order, not a fitted regression and not a claim that the
survivor is faster. Stage 1 timing cannot alter dominance or action.

## Fixed Stage 1 cases

- `fixed_2009_all_12`: 128, 256, 512 MiB;
- `variable_2010_all_12`: 128, 256, 512, 1024 MiB;
- `evolution_through_2011`: 128, 256, 512, 1024 MiB.

The schedule contains three CPU references, eleven GPU warm-ups, and twenty-two
measured GPU executions in two randomized complete blocks.

## Per-run falsification

Immediately before every run, reconstruct the scan and require an exact match to the
frozen registry for configured value, planned tasks/ranges, useful tasks, empty tasks/ranges,
and physical/useful hashes. Any difference falsifies compliance and aborts the accepted
package.

Accepted runs also require canonical CPU/GPU equality, GPU scan plans, successful and
complete task/stage/application lifecycles, no executor removal, and zero standard or
RAPIDS spill/retry evidence.

Sparse RAPIDS accumulators are interpreted as zero when absent only because producer
semantics and the RAPIDS JAR/code identity are frozen. Any nonzero update is an
immediate rejection. Standard Spark spill bytes are checked independently.

## Descriptive outputs

The validator reports, without performance inference:

- each episode's dominated and surviving candidates;
- `SELECT` or `ABSTAIN`/fallback;
- raw and median scan/query timings;
- paired within-block differences for frozen dominated/dominator pairs;
- mechanism prediction coverage/errors;
- missing `PULocationID` row values;
- missing-column materialization bytes as `UNMODELED`.

Performance is always `EXPLORATORY_INCONCLUSIVE`.

## Identity and replay

The preregistration snapshot pins every input and specification, all runner and
validator code, every Spark 3.5.5 `dist/jars` JAR, the RAPIDS assembly JAR, repository HEAD,
JVM and GPU identities, stable CPU topology, process/cgroup affinity, and total host
memory. The verifier must pass before execution.

Stage 1 validation is replayed byte-for-byte. The wrapper journal is finalized,
validated into a wrapper verdict, and checksummed with the final package.

## Priority decision

Current verdict: **MEASURE AFTER PREREGISTRATION VERIFICATION**.

The snapshot is generated only after final documentation. Execution may then test
mechanism compliance and the structural selector. No result can authorize a performance
ranking or production use.
