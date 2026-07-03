# Prospective bounded-regret transfer rerun

The 512-MiB policy was frozen from the new 2009 dynamic-concurrency sweep before this run. The descriptive point-median criterion required no more than 10% regret in every 2010/2011 episode/query cell against the restricted {128, 512, 2048, 4096}-MiB comparator set. The epochs were held out from that 2009 selection calculation, but had been studied in earlier static-concurrency experiments; this is not an untouched independent holdout.

- Measured runs: 80
- Stable result hashes by episode/query: True
- Retry / split-retry / spill bytes: 0 / 0 / 0
- Point-median regret criterion passes: True
- Lifecycle validation passes: True
- Overall preregistered acceptance passes: True
- Maximum comparator-set point-median regret: 7.07%

| Episode | Query | Best comparator MiB | Best median ms | 512 median ms | 512 regret | Pass |
|---|---|---:|---:|---:|---:|---:|
| test_2011 | common | 2048 | 273.2 | 277.2 | 1.46% | True |
| test_2011 | variable_width | 512 | 309.8 | 309.8 | 0.00% | True |
| validation_2010 | common | 2048 | 268.6 | 287.5 | 7.07% | True |
| validation_2010 | variable_width | 512 | 341.5 | 341.5 | 0.00% | True |

Paired block effects relative to the frozen 512-MiB policy; positive means the comparator was slower:

| Episode | Query | Comparator MiB | Geometric mean change | Paired log-ratio 95% CI | Exact sign p | Holm p |
|---|---|---:|---:|---:|---:|---:|
| test_2011 | common | 128 | 39.5% | [24.9%, 55.9%] | 0.0625 | 0.1875 |
| test_2011 | common | 2048 | -2.3% | [-15.8%, 13.4%] | 0.8125 | 0.8125 |
| test_2011 | common | 4096 | 31.8% | [13.5%, 53.1%] | 0.0625 | 0.1875 |
| test_2011 | variable_width | 128 | 33.3% | [20.4%, 47.6%] | 0.0625 | 0.1875 |
| test_2011 | variable_width | 2048 | 7.6% | [-1.7%, 17.7%] | 0.1250 | 0.1875 |
| test_2011 | variable_width | 4096 | 43.6% | [33.6%, 54.4%] | 0.0625 | 0.1875 |
| validation_2010 | common | 128 | 101.4% | [81.6%, 123.4%] | 0.0625 | 0.1875 |
| validation_2010 | common | 2048 | -9.9% | [-23.7%, 6.5%] | 0.1875 | 0.3750 |
| validation_2010 | common | 4096 | 0.3% | [-12.3%, 14.6%] | 1.0000 | 1.0000 |
| validation_2010 | variable_width | 128 | 93.1% | [66.3%, 124.2%] | 0.0625 | 0.1875 |
| validation_2010 | variable_width | 2048 | 2.0% | [-15.5%, 23.2%] | 0.7500 | 0.7500 |
| validation_2010 | variable_width | 4096 | 5.9% | [-6.1%, 19.5%] | 0.2500 | 0.5000 |

The preregistered descriptive criterion passes across these two schema/time epochs. Five blocks do not confidence-bound true regret below 10%, and the comparator oracle is restricted. This does not promote 512 MiB to a universal default; the physical-layout experiment shows that table layout can move the useful region.
