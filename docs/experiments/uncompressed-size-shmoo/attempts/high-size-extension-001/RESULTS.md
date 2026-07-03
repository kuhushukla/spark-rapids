# High-size sequential extension

Status: **MEASURED LOCALLY; SEQUENTIAL EXPLORATION**

All 120 runs completed, including 108 measured runs in 36 complete cells.
Result hashes were stable; retry, split-retry, and spill totals were 0, 0, and 0 bytes.

| Episode | Query | Best MiB | Within 5% | Smallest |
|---|---|---:|---|---:|
| train_2009 | common | 4096 | 4096 | 4096 |
| train_2009 | filtered | 4096 | 4096 | 4096 |
| train_2009 | variable_width | 2048 | 2048, 4096 | 2048 |
| train_2009 | schema_evolution | 4096 | 4096 | 4096 |
| validation_2010 | common | 4096 | 4096 | 4096 |
| validation_2010 | filtered | 4096 | 4096 | 4096 |
| validation_2010 | variable_width | 2048 | 2048, 4096, 8192 | 2048 |
| validation_2010 | schema_evolution | 4096 | 4096 | 4096 |
| test_2011 | common | 4096 | 2048, 4096, 8192 | 2048 |
| test_2011 | filtered | 2048 | 2048 | 2048 |
| test_2011 | variable_width | 4096 | 2048, 4096 | 2048 |
| test_2011 | schema_evolution | 2048 | 2048, 4096 | 2048 |

## 2009-selected candidate regret

| Query | Selected MiB | 2010 regret | 2011 regret |
|---|---:|---:|---:|
| common | 4096 | 0.00% | 0.00% |
| filtered | 4096 | 0.00% | 14.98% |
| variable_width | 2048 | 0.00% | 0.08% |
| schema_evolution | 4096 | 0.00% | 3.15% |

Per-cell decoded bytes, rows, batch counts, footprint, and timings are in
analysis/validated-extension.json. This extension was triggered after inspecting
the primary range, so it brackets hypotheses but is not a jointly preregistered
confirmatory comparison.
