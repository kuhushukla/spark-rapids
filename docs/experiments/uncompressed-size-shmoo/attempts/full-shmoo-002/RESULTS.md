# Full uncompressed-size shmoo results

Status: **MEASURED LOCALLY; EXPLORATORY, NOT A PRODUCTION DEFAULT**

All 264 runs completed; 252 measured runs form 84 complete cells with three
randomized blocks. Results were stable within every episode/query. Observed
retry, split-retry, and spill totals are 0, 0, and 0 bytes.

## First-principles byte prediction

For the two-nullable-fixed-width scan, 2*align64(8*rows)+2*align64(ceil(rows/8)) predicted one-batch task
footprint with 0.000439% MAPE and 112 bytes maximum absolute error over 9834/9846 task observations.
These observations represent 1639 unique planned one-batch task layouts; footer-row prediction max error was 0 rows.

## Exploratory knees

| Episode | Query | Best observed MiB | Within 5% | Smallest within 5% |
|---|---|---:|---|---:|
| train_2009 | common | 2048 | 2048 | 2048 |
| train_2009 | filtered | 2048 | 2048 | 2048 |
| train_2009 | variable_width | 2048 | 2048 | 2048 |
| train_2009 | schema_evolution | 2048 | 2048 | 2048 |
| validation_2010 | common | 2048 | 2048 | 2048 |
| validation_2010 | filtered | 2048 | 2048 | 2048 |
| validation_2010 | variable_width | 2048 | 2048 | 2048 |
| validation_2010 | schema_evolution | 2048 | 2048 | 2048 |
| test_2011 | common | 2048 | 2048 | 2048 |
| test_2011 | filtered | 2048 | 2048 | 2048 |
| test_2011 | variable_width | 2048 | 2048 | 2048 |
| test_2011 | schema_evolution | 2048 | 2048 | 2048 |

These are hypotheses for confirmation, not selected defaults. Bytes and rows are
strongly coupled within each fixed schema, so model comparison must use the
cross-query/schema holdouts rather than claim causal separation from one curve.

## Chronological transfer

| Query | 2010 row MAPE | 2010 C_split MAPE | 2010 read MAPE | 2011 row MAPE | 2011 C_split MAPE | 2011 read MAPE |
|---|---:|---:|---:|---:|---:|---:|
| common | 0.00% | 2.62% | 1.14% | 0.00% | 57.07% | 1.76% |
| filtered | 0.00% | 2.62% | 1.14% | 0.00% | 57.07% | 1.76% |
| variable_width | 6.62% | 6.61% | 5.32% | 2.25% | 56.12% | 10.17% |
| schema_evolution | 0.00% | 2.62% | 1.14% | 0.00% | 57.07% | 54.68% |

## Post-filter survivor transfer

| Episode | surviving rows MAPE | surviving bytes MAPE | C_split/U_survive MAPE |
|---|---:|---:|---:|
| validation_2010 | 0.96% | 0.96% | 3.40% |
| test_2011 | 1.02% | 1.03% | 56.76% |

Complete per-cell and held-out model results are in analysis/validated-analysis.json.
