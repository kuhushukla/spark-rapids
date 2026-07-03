# Bathtub/plateau reanalysis of the committed shmoo

## Verdict

The existing runs support a strong small-partition ramp and a high-size turnover, but they do **not** establish the hypothesized broad 4–16× plateau or identify an admission/memory cliff. All measured GPU tasks reported maximum concurrency one, and no retry/spill event occurred. A two-wall controller therefore remains a useful hypothesis requiring a concurrency-enabled sweep.

## Default-regret screen

- Annual query cells: 12
- Spark 3.5.5 default (128 MiB) inside the descriptive 5% set: 0/12
- Bridge-adjusted default regret: min 298.81%, median 689.35%, max 802.82%.
- 8192 MiB slower than 4096 MiB in 12/12 cells.
- More than one adjacent candidate inside the 5% region: 5/12 cells; a contiguous 4× region: 2/12.
- At the default, median scan-span/query share was 98.2% and median GPU semaphore-holding/scan-span share was 99.6%.

These are exploratory medians, not confidence-bounded production regret. The original and extension attempts were separate applications; high-size times are normalized through their shared 2048-MiB cell.

## Per-cell bounds

| Episode | Query | Best observed MiB | Contiguous 5% region MiB | Default regret | Default CV | Approx. n=10 detectable effect | 90% batch-fill candidate |
|---|---|---:|---|---:|---:|---:|---:|
| test_2011 | common | 4096 | 2048–8192 | 366.48% | 0.87% | 1.09% | 2048 |
| test_2011 | filtered | 2048 | 2048–2048 | 392.73% | 1.06% | 1.33% | 2048 |
| test_2011 | schema_evolution | 2048 | 2048–4096 | 298.81% | 2.21% | 2.77% | 1024 |
| test_2011 | variable_width | 4096 | 2048–4096 | 351.85% | 0.29% | 0.37% | 1024 |
| train_2009 | common | 4096 | 4096–4096 | 727.76% | 2.44% | 3.05% | 4096 |
| train_2009 | filtered | 4096 | 4096–4096 | 757.20% | 1.15% | 1.44% | 4096 |
| train_2009 | schema_evolution | 4096 | 4096–4096 | 796.85% | 1.59% | 1.99% | 4096 |
| train_2009 | variable_width | 2048 | 2048–4096 | 644.57% | 0.54% | 0.67% | 4096 |
| validation_2010 | common | 4096 | 4096–4096 | 763.01% | 0.69% | 0.86% | 4096 |
| validation_2010 | filtered | 4096 | 4096–4096 | 802.82% | 1.51% | 1.90% | 4096 |
| validation_2010 | schema_evolution | 4096 | 4096–4096 | 793.61% | 0.94% | 1.18% | 4096 |
| validation_2010 | variable_width | 2048 | 2048–8192 | 650.95% | 0.84% | 1.05% | 4096 |

The detectable-effect column is only a normal-approximation planning number based on three observed default runs. Ten dedicated default repetitions are still required before preregistering a confirmatory effect threshold.

## Mechanism conclusions

1. **Small side:** elapsed time is fit against actual scan-task count for candidates through 512 MiB. The slope is an effective incremental task cost, not pure setup time.
2. **Batch filling:** the first candidate whose observed maximum batch reaches 90% of the 1-GiB target is reported independently from partition-size performance.
3. **Admission:** unidentifiable here because static concurrency was pinned to one.
4. **Upper wall:** censored; the sweep observed no spill/retry/OOM. A slowdown at 8192 MiB is performance evidence, not a measured memory cliff.
5. **Critical-path gate:** scan task span and GPU semaphore-holding share are now recoverable from the event log. They are proxies; neither is GPU SM utilization.

Across the 12 cells, the small-side fit estimated 31.6–46.0 effective additional milliseconds per scan task (median 37.9 ms, minimum R² 0.99975). This unusually linear result is strong evidence for a task-count mechanism in this static-concurrency lane, but the slope still combines setup, scheduling, and any other effect correlated with task count.

## Required next experiment

Run a separately preregistered, concurrency-enabled mechanism sweep. Include ten default repetitions, log-spaced partition sizes, at least narrow fixed-width and wide string projections, and a fixed-large-partition batch-size sweep. Stop escalation at the first retry/spill/split event. Validate a bounded plateau policy on held-out runs instead of selecting and validating a point optimum on the same sweep.
