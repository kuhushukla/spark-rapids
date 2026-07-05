<!--
Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Rolling 12-month split autotuning

## Status

PREREGISTERED. The protocol and full schedule are frozen before any GPU treatment run.

## Question

Can a per-table split policy, using only normal reader inputs and measurements from
its own prior completed reads, adapt as a 12-month query window advances one month at
a time?

The enabled policy is compared within every rolling window with fixed 128 MiB and
1 GiB `spark.sql.files.maxPartitionBytes` controls.

## Information boundary

The selector may use only:

- the read/data schema, projection, pushed predicate, file format, and Spark/RAPIDS
  configuration already supplied to the scan;
- selected file paths and encoded lengths already produced by Spark file listing;
- current batch target and a live task-slot hint;
- records produced by earlier successful enabled reads of the same logical table.

It must not open Parquet footers, read catalog column statistics, sample current rows,
use the current window's control outcomes, or inspect future windows. Runtime
measurements update history only after the enabled read finishes.

## Online policy under test

The first window has no history and uses 128 MiB. Later windows use at most the last
12 successful enabled observations. For each observation, the policy stores decoded
scan bytes and rows divided by the selected files' listed encoded bytes. A
recency-weighted median, with a three-window half-life, predicts the next window.

The raw split target is:

```text
splitBytes = rapidsBatchTargetBytes / predictedDecodedBytesPerListedByte
```

It is rounded to 16 MiB and clamped to [64 MiB, 1 GiB]. This is intentionally a
small, implementable data-shape controller. It does not use footer layout, control-arm
timings, or an offline oracle. It is not claimed to find the global performance
optimum.

Both predicted decoded rows and bytes are recorded. Bytes drive this first policy;
row prediction is evaluated independently so later policies can add an empirical row
floor without silently fitting it on the same results.

## Workloads

Every taxi workload uses exact monthly files. The Freddie workload uses the normal
`period` predicate and only the year partition directories intersecting the rolling
window.

- Yellow Taxi: all contiguous 12-month windows from 2009-01 onward.
- Green Taxi: all contiguous 12-month windows from 2014-01 onward.
- For-Hire Vehicle: all contiguous 12-month windows from 2015-01 onward.
- High-Volume For-Hire Vehicle: all contiguous 12-month windows from 2019-02 onward.
- Freddie Mac `stacr_dnhq`: monthly predicates from 2013-07 through the available
  2026-01 reporting range.

Queries aggregate projected columns so the scan output is consumed. Treatment order
is randomized independently within each window from the frozen schedule.

## Experimental unit and measurements

A rolling window is a paired block. Each block contains one measured enabled run,
one fixed-128 run, and one fixed-1024 run. One excluded warm-up of every treatment is
run on the first window of each table. The large number of temporal blocks supplies
replication; there is no repeated-run median within a block.

Primary reporting is estimation-only:

- one-step decoded-byte and decoded-row absolute percentage error;
- paired whole-query and scan-stage ratios for enabled versus each fixed control;
- enabled regret relative to the faster control, clearly labeled as a noisy
  single-run quantity;
- selected split trajectory and change lag;
- retry, split-retry, spill, and footprint observations.

Report percentile bootstrap 95% intervals across temporal blocks. Windows are
autocorrelated, so use a 12-window moving-block bootstrap. Do not treat the number of
windows as independent IID replication.

Frozen diagnostic thresholds, reported separately rather than collapsed:

- decoded bytes: median APE <= 20% and p90 <= 35%;
- decoded rows: median APE <= 20% and p90 <= 35%;
- performance robustness: median enabled regret <= 10% and p90 <= 25%;
- safety: zero retry, split-retry, host/disk spill, and Spark spill.

The first cold-start window is excluded from prediction-error thresholds but retained
in performance reporting.

## Correctness and safety

All three GPU treatments in a window must have identical canonical results. CPU
references are run for the first, middle, and final window of every table and must
match the GPU result. Any mismatch, fatal OOM, executor loss, or nonzero retry/spill
invalidates the affected table result and stops subsequent work for that table.

All Spark scratch, warehouse, event-log, and temporary storage is under `/data/tmp`.
The encrypted workspace drive is not used for shuffle or spill.

## Interpretation boundary

This test can establish whether the cheap historical ratios track a moving window and
whether the simple batch-fill policy is robust relative to the two fixed controls on
this hardware and query family. It cannot establish a universal optimum, object-store
behavior, distributed shuffle behavior, or safety on a smaller GPU.
