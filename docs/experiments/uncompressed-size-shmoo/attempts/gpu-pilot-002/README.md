# GPU pilot attempt 002

Status: **MECHANISM PILOT PASSED; NOT PERFORMANCE INFERENCE**

Spark 3.5.5 ran RAPIDS revision `4c66f7214` on the first three 2009 months with
static GPU concurrency one and DEBUG metrics. Configured 128, 256, and 512 MiB
treatments produced 21, 11, and 5 useful scan tasks. Their maximum observed decoded
GPU batches were 32,714,672; 65,429,184; and 139,036,864 bytes. Every scan task emitted
one batch, so per-task volume and maximum batch footprint coincide in this pilot.

Single ordered elapsed observations were 684.3, 436.5, and 258.9 ms. They demonstrate
treatment response but are not a shmoo conclusion: there was no blocking, repetition,
or time/query holdout.

The decoded size follows the fixed-width first-principles expectation closely:
`U = rows * (8-byte long + 8-byte double + two nullable validity bits)`, or about
16.25 bytes/row. This is a hypothesis for the full chronological test, not a fitted
constant.

The 128-MiB result hash differs from 256/512 because the query summed floating-point
`trip_distance`; changing partition grouping changes addition order and final bits.
The full frozen query replaces that check with deterministic row/count aggregates while
still reading the same column. This pilot is therefore retained as mechanism evidence,
not as a cross-treatment correctness result.
