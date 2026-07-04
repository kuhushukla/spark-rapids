# Model validation ledger

This ledger separates mechanism validation from predictive validation. A mechanism can
be real while a numerical predictor is still unqualified.

| Model statement | Evidence | Verdict |
|---|---|---|
| Historical encoded-to-decoded ratios predict later taxi snapshots | Parent experiment prospective 2010/2011 holdouts | Supported for the tested projections; residual bounds remain context-specific |
| Small partitions inflate fixed decode/task work | 128 MiB has 10,962 kernels and 444 ms decode-window union versus 735 and 110 ms at 2 GiB | Supported |
| The high-end wall is more GPU computation | Kernel service is 34.3 ms at 2 GiB and 30.1 ms at 16 GiB | Rejected |
| The high-end wall is lost task-level pipeline opportunity | Useful tasks fall 5 -> 1 while Spark stage wall rises 279 -> 469 ms and GPU busy changes only 31.3 -> 30.1 ms | Supported; kernel-overlap loss alone explains only about 3 ms |
| Warm-local filesystem reading is the bottleneck | DEBUG summed filesystem reads are roughly 32--42 ms across 2--32 GiB | Rejected for this context; not transferable to object storage |
| GPU holders are linear GPU throughput lanes | Kernel service/busy overlap is about 1.10 at 2 GiB, not four | Rejected; holders are a feasibility cap |
| Additive component sums predict stage wall | Read/decode/GPU work overlaps across tasks | Rejected as a wall model |
| Max-capacity plus longest-task bound can be calibrated to 10% | Three profiled cells, no profile repetitions, incomplete task-level inputs | Not established |

The implemented POC therefore requires an externally measured GPU-overlap capacity,
independent read/task overlap, launched and useful task counts, a fitted fixed cost per
task, a candidate longest-task estimate, and externally safe decoded/footprint bounds.
It refuses size extrapolation and does not expose uncertainty below five residual
samples.

The later fresh-query trial demonstrates that the downstream fingerprint is essential:
aggregate/filter median curves favored larger partitions, while self-join/window curves
favored smaller ones. Its 512-MiB minimax fallback had 12.50% worst point regret and
failed a universal 10% criterion. That trial is descriptive because its preregistration
was not immutably bound before execution.

A separate source-layout treatment launched 46 tasks at 128 MiB; 34 emitted no scan
output yet accumulated about 3,038 ms of GPU-holding time. This motivates a reader code
optimization, not merely another tuning rule. Before changing acquisition, trace whether
pruning proves emptiness before the relevant reader's semaphore call; discovering
emptiness through GPU decode cannot safely skip acquisition.

The next confirmatory experiment must record task-level service/timelines, file and row
group assignment, launched/empty tasks, survivor batches, scan-pruning versus SQL-filter
time, read requests/ranges, and upper decoded/footprint residuals. Train on one snapshot
and validate on later snapshots and modified predicates without refitting. The
preregistered acceptance criteria remain median stage error <=10%, p90 <=15%, correct
bottleneck family, and correct exclusion of both walls.
