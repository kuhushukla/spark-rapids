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

# Rolling 12-month split autotuning results

## Bounded verdict

The frozen run completed all 678 windows and 2,034 measured GPU treatments.
It supports the following claims on Spark 3.5.5, RAPIDS 26.08 development
artifacts, one local A6000-class GPU, local storage with serial warm-up and an
uncontrolled/evolving shared page cache, and the registered
scan/aggregate queries:

- A same-table historical ratio using only selected file lengths and prior
  completed scan metrics can track decoded rows and bytes through rolling
  table evolution.
- The frozen batch-fill policy is much better than fixed 128 MiB on the large
  Yellow, HVFHV, and Freddie workloads, and generally no worse than fixed
  1 GiB at the median.
- The policy is not robustly within 25% of the better noisy control on Yellow:
  its p90 descriptive regret is 31.75%.
- The experiment validates the Python online algorithm. It does not validate
  the Scala POC's per-read integration, concurrent planning, multi-scan
  queries, or production persistence lifecycle.

The machine result is [analysis/result.json](analysis/result.json). Post-hoc
work is [analysis/exploratory.json](analysis/exploratory.json) and cannot
replace the frozen verdict.

## Frozen results

| Logical table | Windows | Byte APE median / p90 | Row APE median / p90 | Enabled / 128 median | Enabled / 1 GiB median | Descriptive regret median / p90 |
|---|---:|---:|---:|---:|---:|---:|
| Yellow | 198 | 0.43% / 2.59% | 0.43% / 2.59% | 0.549 | 1.038 | 3.85% / 31.75% |
| Green | 138 | 0.60% / 3.19% | 0.60% / 3.19% | 0.979 | 0.998 | 0.69% / 10.29% |
| FHV | 125 | 0.97% / 8.05% | 1.10% / 8.13% | 0.816 | 0.978 | 0.00% / 7.11% |
| HVFHV | 77 | 0.29% / 1.60% | 0.29% / 1.60% | 0.485 | 0.996 | 0.00% / 6.68% |
| Freddie CRT | 140 | 5.63% / 34.32% | 5.58% / 34.32% | 0.265 | 0.990 | 0.00% / 9.96% |

The regret statistic compares enabled with the minimum of one 128 MiB and one
1 GiB observation in the same window. It is upward-biased by selecting the
faster noisy control and is descriptive only. Paired log ratios against each
control are the defensible treatment contrasts.

Moving-block confidence intervals were recomputed at block lengths 6, 12, 24,
and 36. Conclusions versus 128 MiB were stable. Yellow was consistently slower
than 1 GiB at the median; Green and HVFHV were effectively tied with 1 GiB;
FHV was slightly faster. Freddie's comparison with 1 GiB was sensitive around
zero. Adjacent shape observations had lag-1 autocorrelation from 0.51 to 0.99,
so raw window counts are not IID sample counts.

Treatment position and predecessor strata showed nontrivial differences, especially
for HVFHV. Frozen randomization protects the average contrast in expectation, but this
run cannot distinguish evolving page-cache carryover from temporal workload allocation.
Per-position results are diagnostics, not separate causal estimates.

## What information the selector used

The decision used only:

- table identity supplied by the experiment;
- current selected paths and their normal filesystem-listed lengths;
- projection/query family fixed by the registered workload;
- RAPIDS batch target and split bounds;
- decoded rows and bytes from prior successful enabled reads of that table.

It did not open Parquet footers, run a metadata query, sample current rows,
use catalog column statistics, use current control results, or inspect future
windows. All scratch, shuffle, event logs, warehouse data, and JVM temporary
files were under `/data/tmp`.

The decision itself examines at most 12 history records and is
`O(12 log 12)`. This experiment did not measure production history lookup,
locking, or persistence overhead.

## Average versus recent history

The frozen policy used a recency-weighted median over up to 12 prior windows.
A post-hoc prequential replay compared:

- last observation;
- rolling mean and median over 3, 6, and 12 observations;
- EWMA with half-lives 1.5, 3, and 6;
- a robust innovation-clipped EWMA.

Every prediction for window t used observations strictly before t. Yellow was
the exploratory training table. The last-observation estimator won by median
absolute log error and was then evaluated unchanged on the other tables.

| Logical table | Last-observation byte APE median / p90 / max |
|---|---:|
| Yellow | 0.16% / 0.82% / 11.03% |
| Green | 0.20% / 1.14% / 2.48% |
| FHV | 0.36% / 1.79% / 11.38% |
| HVFHV | 0.12% / 0.53% / 0.62% |
| Freddie CRT | 2.43% / 38.86% / 148.90% |

A 12-month query result already overlaps the next result by 11 months.
Additional averaging creates roughly triangular multi-year memory and lags
real change. Older observations are more useful for one-step residual
distributions, uncertainty, and drift detection than for the point estimate.

Freddie is the important exception: last observation improved the median but
not the tail. Twelve windows selected only one year directory and had frozen
median/p90 error 36.45%/41.93%; the 127 two-directory windows had
5.03%/25.11%. Selected-path count and predicate/file-selection regime are therefore cheap
candidate features or uncertainty triggers requiring prospective validation.
They are available from normal planning; no footer inspection is required.

## Divergence and abstention

The exploratory detector diagnosed residuals from the frozen weighted-median
policy. It used only prior one-step errors, required ten errors
before estimating uncertainty, and required two of three same-direction misses
above both the historical p90 and a practical 10% threshold. It found:

- Yellow regime divergence in 2010;
- FHV divergence in 2016;
- Freddie divergence in 2025;
- no practical divergence in Green or HVFHV.

These trigger dates do not validate drift/reset behavior for the post-hoc
latest-observation estimator. That combination must be frozen and tested
prospectively. A proposed production rule would:

1. start with the latest compatible same-table observation;
2. build uncertainty from prior one-step residuals;
3. clip one isolated innovation rather than erasing history;
4. after sustained same-direction divergence, shorten history to the newest
   regime or discard a weak transferred prior;
5. return the ordinary Spark default when the residual interval is too wide.

A center can start after one observation and is usually useful after 3--5.
Uncertainty and drift decisions need at least 10, preferably 20, prior
one-step errors. There is no honest universal performance sample count; it
depends on paired timing variance and effective sample size after temporal
correlation.

## Metric and actuator diagnostics

Decoded rows were identical across all three treatments in every window.
Aggregate decoded device bytes were also stable enough for this workload:
the worst treatment-relative spread was 0.000221%. Batch counts, however,
changed by as much as 8x. The byte metric is therefore empirically
usable here but is not definitionally actuator-independent; future readers and
variable-width paths must repeat this invariance check.

The 128 MiB and 1 GiB controls produced identical planned-task and batch counts
in 74 of 138 Green windows. This demonstrates a real no-op region. A production
controller should persist planned tasks, useful/output tasks, output batches,
and maximum batch size, then abstain when split changes do not move the
actuator response.

Windows where enabled selected the same setting as a control were used as
natural timing replicates. Their p90 absolute log differences ranged from
3.5% to 12.8% by table. This explains some individual-window apparent regret
and is another reason not to select an optimum from one timing per setting.

## Correctness and safety evidence

- Every three-treatment result hash matched.
- CPU references for the first, middle, and final window of every table
  matched the GPU result.
- Direct in-process decoded rows/bytes and planned tasks matched the event-log
  extraction.
- Standard Spark memory and disk spill metrics were present and zero.
- No nonzero RAPIDS retry or spill update was observed.

RAPIDS retry/spill task accumulators were sparse and absent from these task
records. Absence is not independently equivalent to a present zero counter.
The pinned producer semantics may define sparse absence as zero, but the
portable evidence statement is “no observed nonzero update,” not “every
counter was present and zero.”

## Integration findings

Independent reviewers confirmed the experiment's causal isolation and found
the following production gaps:

- The tested algorithm is Python and changes the session-wide Spark setting
  before constructing each DataFrame. It is not the per-read plugin actuator.
- `PerformanceHistory` had no production caller.
- The experiment's cheap denominator is listed selected-file bytes, while the
  Scala POC called its field compressed bytes read.
- Codec cannot be a required match because it is generally unavailable without
  footer work.
- Learning currently requires DEBUG scan metrics; production needs an
  explicitly low-overhead tuner counter or must account for that cost.
- The hardcoded table label does not validate real table identity,
  projection/schema compatibility, dynamic partition pruning, or multiple
  table reads in one query.
- The Freddie runner selected intersecting year directories explicitly. Its
  result is valid for already-selected paths, not proof that a root-table query
  derives the same paths automatically.

The Scala POC was corrected after the run to separate listed bytes from actual
bytes read, make codec non-gating, use the latest compatible same-table ratio,
and derive upper bounds from historical one-step increases. These changes are
supported by post-hoc algorithm evidence and unit tests; they have not received
a new causal runtime validation through a per-read plugin hook.

## Evidence and replay

Compact journals, results, histories, CPU validations, and derived analysis are
committed under [raw](raw), [analysis](analysis), and [provenance](provenance).
The complete task summaries, compressed event logs, stdout, and uncompressed
Spark logs remain at:

`/data/tmp/rolling-split-autotuning-run-001`

[external-artifact-sha256.txt](provenance/external-artifact-sha256.txt)
authenticates the original package. The [postflight validator](analysis/postflight-validation.json) confirmed that
the frozen file paths, sizes, and mtimes still matched after execution.
