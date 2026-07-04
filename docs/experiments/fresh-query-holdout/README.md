# Fresh query-shape holdout

Status: **EXECUTED, DESCRIPTIVE, PROVENANCE-LIMITED**

This experiment tests whether the frozen 512-MiB fallback transfers to four query shapes
that were absent from the original scan/aggregation study. The numerical evidence is
internally consistent and was independently recomputed from the captured artifacts. However, the supplied
`preregistration/prereg.json` was never committed or hashed into a run artifact before
execution. Its claim that it was frozen before the pilot is therefore self-described,
not immutably proven. This package must not be cited as a cryptographically verified
preregistration.

## Frozen-style protocol actually executed

- RTX A6000, Spark 3.5.5 `local[8]`, warm local storage;
- AQE disabled;
- dynamic GPU admission enabled, initial concurrency four;
- RAPIDS batch target 1 GiB and reader soft limit 2 GiB;
- partition candidates 128, 512, 2,048, and 8,192 MiB;
- four warmups plus sixteen randomized measured cells per block;
- three blocks with seeds 20260711, 20260712, and 20260713;
- 48 measured runs total.

`analysis/analysis-summary-original.json` preserves the supplied result; the versioned
analyzer reproduces its medians and adds cell, treatment, and minimax details in
`analysis/analysis-summary.json`.

The recorded order exactly matches `random.Random(seed).shuffle` for every block.
All attempts are attempt zero. Every query has one result hash across all sizes and
blocks. No GPU retry, split-retry, host/disk spill, Spark spill, failed task, executor
loss, or missing cell was found.

## Queries

- q1: full-corpus multi-key aggregation;
- q2: full-corpus self-join of an aggregated dimension back to the scan;
- q3: 2011-only partitioned window/top-N followed by aggregation;
- q4: full-corpus wide projection with a highly selective filter and global aggregate.

The treatment changed scan tasks at every adjacent size:

| Query | 128 MiB | 512 MiB | 2,048 MiB | 8,192 MiB |
|---|---:|---:|---:|---:|
| q1 / q4 | 212 | 50 | 13 | 5 |
| q2, each of two scans | 212 | 50 | 13 | 5 |
| q3 | 39 | 9 | 3 | 1 |

## Results

Point medians from three blocks:

| Query | 128 MiB | 512 MiB | 2,048 MiB | 8,192 MiB | Best | 512 regret |
|---|---:|---:|---:|---:|---:|---:|
| q1 multi-key aggregate | 9.8937 s | 8.6411 s | 8.4733 s | 7.6812 s | 8,192 | 12.50% |
| q2 self-join | 16.5429 s | 18.0076 s | 20.6229 s | 21.8336 s | 128 | 8.85% |
| q3 window/top-N | 4.5107 s | 4.8701 s | 5.2215 s | 5.9551 s | 128 | 7.97% |
| q4 selective filter | 4.4413 s | 3.1647 s | 3.0023 s | 2.8942 s | 8,192 | 9.35% |

The median curves are monotone but individual blocks are not universally monotone.
With only three blocks this is descriptive evidence, not a confidence-bounded
query-shape interaction.

Worst point-median regret across these queries is:

- 128 MiB: 53.46%;
- 512 MiB: 12.50%;
- 2,048 MiB: 24.66%;
- 8,192 MiB: 32.02%.

Thus 512 MiB remains the restricted-set minimax fallback, but it fails the stated
within-10% criterion. No single tested value is within 10% of best on all four shapes.

## Interpretation

Partition treatment changed map-stage runnable parallelism as well as scan granularity.
The q2/q3 median slowdown is associated with collapsing 212 tasks to 5—or 39 to 1—while
substantial aggregation, shuffle-write, or window/sort work remains in the query. The
trial does not isolate downstream parallelism as the sole cause. Conversely, q1/q4
medians improved as partitions grew, but the trial does not isolate setup amortization
as the sole cause.

No single tested candidate met the 10% point-median objective on these four shapes. This
supports including a downstream/query-shape dimension in the context; it does not prove
that the proposed fingerprint is sufficient or predictive. A conservative fallback remains useful only when the model
abstains.

AQE was disabled to match the earlier protocol. Production AQE can coalesce shuffle
partitions and may change the q2/q3 slope. The highest-value immediate follow-up is an
AQE-on paired rerun of q2/q3 with the same treatment and correctness hashes.

## Noise and equivalence

The original 6.8% versus 7.1% minimax distinction is below the experiment's resolving
power. `analysis/winner-bias-simulation.json` simulates five truly equal independent
lognormal candidates at 6% CV with five repetitions. A fixed candidate shows 3.86%
mean apparent regret and 8.68% p90 regret solely because the observed winner is selected
from noisy candidates.

This simulation is a sensitivity analysis, not a model of every correlation in the
paired benchmark. It demonstrates why tiny point-regret differences should not be
ranked. Future confirmation should predeclare a ±10% equivalence/noninferiority band,
use roughly ten paired randomized blocks, narrow the candidate set, and use TOST or an
equivalent confidence-interval decision. Report "equivalent inside the band" rather
than selecting a noisy winner.

## Reproduction

From this directory:

```bash
python3 scripts/parse_eventlogs.py \
  --eventlog-dir raw/eventlogs \
  --output /tmp/fresh-eventlog-metrics.json
cmp /tmp/fresh-eventlog-metrics.json analysis/eventlog-metrics.json

python3 scripts/analyze.py --root . --output /tmp/fresh-analysis.json
cmp /tmp/fresh-analysis.json analysis/analysis-summary.json

python3 scripts/simulate_winner_bias.py --output /tmp/winner-bias.json
cmp /tmp/winner-bias.json analysis/winner-bias-simulation.json

sha256sum -c provenance/manifest.txt
```

The committed runner is retained for audit, but its exact source hash was not embedded
in the event logs. The event logs independently bind the executed block numbers, seeds,
Spark settings, job groups, metrics, and results.

## Decision tiers

- Tier 1, descriptive restricted-set minimax candidate: 512 MiB was never worst in this
  tested set, with 12.5% worst point regret here. A broader 15–25% guarantee is not statistically proven,
  but this is useful descriptive support.
- Tier 2, within 5–10%: not achieved by any tested global value. It requires context-keyed
  prediction, downstream fingerprints, explicit fixed/empty-task costs, AQE context,
  uncertainty, and abstention.

The model and code changes accompanying this package add launched-task count and a
modeled per-task fixed term that must be fitted or instrumented externally. Empty-task
semaphore avoidance is a separate potential code optimization and should be evaluated independently from partition tuning.
