# Cumulative growth and mixed-schema results

Status: **MEASURED LOCALLY; EXPLORATORY GROWTH STUDY**

All 80 runs completed; 72 measured runs form 24 complete cells.
Result hashes were stable; observed retry and spill totals were 0 and 0 bytes.

| Window | Query | End | Best MiB | Within 5% | Smallest |
|---|---|---|---:|---|---:|
| growth_12m | common | 2009-12 | 4096 | 4096 | 4096 |
| growth_1m | common | 2009-01 | 8192 | 2048, 4096, 8192 | 2048 |
| growth_24m | common | 2010-12 | 8192 | 4096, 8192 | 4096 |
| growth_36m | common | 2011-12 | 8192 | 2048, 4096, 8192 | 2048 |
| growth_3m | common | 2009-03 | 2048 | 2048, 4096, 8192 | 2048 |
| growth_6m | common | 2009-06 | 2048 | 2048, 4096 | 2048 |
| mixed_evolution_24m | schema_evolution | 2010-12 | 8192 | 4096, 8192 | 4096 |
| mixed_evolution_36m | schema_evolution | 2011-12 | 8192 | 2048, 4096, 8192 | 2048 |

The complete cell table includes task count, decoded byte/row p50 and p95,
batch count, footprint, and all three elapsed observations. Small windows can
collapse multiple configured candidates onto the same physical layout; those
configured labels are not independent treatments.
