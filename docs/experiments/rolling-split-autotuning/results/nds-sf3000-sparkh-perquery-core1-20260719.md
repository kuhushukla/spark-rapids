# NDS SF3000 sparkh — per-query table (core1 vs baseline)

Companion to `nds-sf3000-sparkh-breakdowns-20260719.md`. One row per query, **core1 (1 task/core) vs
autotuner-off baseline**, warm (mean of iterations 2–5). Sources: per-query CSVs + core1 test event log
(`data/nds-sparkh-pcap-core1-20260719_204201/`).

**Columns**
- `OFFwarm` / `ONwarm` — warm query time (s), baseline vs core1.
- `spdup` — OFFwarm / ONwarm ( >1 = core1 faster; <1 = slower ).
- `scans` — number of split decisions the autotuner made in that query.
- `ratio` — of those, how many were **bound by the ratio** (vs the parallelism cap or 64 MB floor).
- `avgBatch` / `%full` — average scan output batch size for that query's scans, and % of the 1 GiB target
  (from `sum output bytes / sum output batches` over the query's scan nodes in the event log).

**Caveats (read before quoting)**
- **`_part` queries (14, 23, 24, 39) show 0 in `scans`/`avgBatch`/`%full`.** Their event-log description is
  `"query14"` (no `_part` suffix), so the join to the per-part CSV rows drops them. **Their timings are
  still correct**; only the scan/batch columns are blank for those rows.
- `avgBatch`/`%full` here are **per-query** (aggregated over that query's scan nodes) — a slightly different
  aggregation than the per-*table* exact numbers in the main doc (e.g. store_sales 505 MiB there). So a
  query's number reflects its specific projection/filter, and the same table can show different batch sizes
  in different queries.
- `%full` and `avgBatch` are from real batch counts (`NUM_OUTPUT_BATCHES`), not the per-task proxy.

**What to point at**
- The **ratio only binds on 5 queries** — all store_sales-heavy: query28 (30), query93 (5), query44 (5),
  query50 (4), query9 (1). Everywhere else `ratio=0`.
- Those ratio-bound queries have the **fullest batches** (query50 87%, query28 84%, query88 72%, query9/96
  65%) **yet aren't faster**: query50 0.93×, query93 0.93×, query28 0.98×. Fuller batches ≠ faster, per query.
- Most queries are ~1.0× (core1 ≈ baseline), consistent with the ratio changing only ~2% of scans overall.

```
query          OFFwarm  ONwarm  spdup scans ratio avgBatch %full
query1            1.05    1.18  0.89x    30     0      15M    1%
query2            1.04    1.06  0.98x    50     0     173M   17%
query3            0.38    0.39  0.97x    15     0     102M   10%
query4            3.56    3.62  0.98x    45     0     144M   14%
query5            1.91    1.95  0.98x    55     0     118M   11%
query6            0.79    0.84  0.94x    35     0      26M    3%
query7            0.94    1.01  0.93x    21     0     312M   30%
query8            0.79    0.66  1.20x    30     0      30M    3%
query9            1.44    1.40  1.03x    10     1     661M   65%
query10           1.17    1.14  1.03x    35     0      13M    1%
query11           2.06    2.20  0.93x    35     0     103M   10%
query12           0.44    0.41  1.06x    15     0       9M    1%
query13           0.86    0.84  1.02x    40     0     381M   37%
query14_part1     4.48    4.58  0.98x     0     0       0M    0%
query14_part2     4.12    4.06  1.02x     0     0       0M    0%
query15           0.97    0.94  1.03x    20     0      18M    2%
query16           1.08    1.11  0.97x    40     0     351M   34%
query17           1.22    1.13  1.08x    35     0      43M    4%
query18           1.35    1.71  0.79x    35     0     176M   17%
query19           0.73    0.83  0.89x    28     0      46M    4%
query20           0.42    0.40  1.05x    15     0      17M    2%
query21           0.35    0.35  1.00x    20     0      71M    7%
query22           0.91    0.96  0.94x    15     0      87M    8%
query23_part1     4.92    5.12  0.96x     0     0       0M    0%
query23_part2     5.58    5.82  0.96x     0     0       0M    0%
query24_part1     5.66    5.75  0.98x     0     0       0M    0%
query24_part2     5.72    5.79  0.99x     0     0       0M    0%
query25           1.17    1.13  1.04x    35     0      38M    4%
query26           0.58    0.61  0.95x    25     0     165M   16%
query27           0.73    0.75  0.97x    25     0     316M   31%
query28           3.56    3.63  0.98x    30    30     857M   84%
query29           2.42    2.39  1.01x    40     0     206M   20%
query30           1.35    1.36  0.99x    30     0      33M    3%
query31           1.41    1.44  0.98x    50     0      22M    2%
query32           0.66    0.71  0.93x    25     0      16M    2%
query33           0.87    0.92  0.95x    35     0      24M    2%
query34           1.57    1.53  1.03x    25     0     136M   13%
query35           1.27    1.18  1.08x    35     0      26M    2%
query36           0.86    0.88  0.98x    20     0     197M   19%
query37           0.44    0.41  1.07x    20     0     123M   12%
query38           1.54    1.48  1.04x    25     0      48M    5%
query39_part1     1.12    1.06  1.06x     0     0       0M    0%
query39_part2     0.92    0.94  0.97x     0     0       0M    0%
query40           0.92    0.95  0.97x    30     0      29M    3%
query41           0.23    0.22  1.05x    10     0      17M    2%
query42           0.25    0.25  1.02x    15     0      29M    3%
query43           0.61    0.61  1.01x    15     0     122M   12%
query44           0.41    0.42  0.98x    10     5      11M    1%
query45           0.84    0.82  1.02x    30     0      14M    1%
query46           0.96    1.13  0.85x    30     0     231M   23%
query47           1.25    1.41  0.89x    20     0     169M   17%
query48           0.67    0.68  0.98x    25     0     275M   27%
query49           1.44    1.37  1.05x    35     0      45M    4%
query50           6.70    7.19  0.93x    25     4     893M   87%
query51           1.13    1.20  0.94x    15     0      76M    7%
query52           0.37    0.39  0.95x    15     0      32M    3%
query53           0.53    0.59  0.91x    20     0     156M   15%
query54           1.18    1.17  1.00x    55     0      28M    3%
query55           0.37    0.37  0.99x    15     0      32M    3%
query56           0.69    0.71  0.97x    35     0      24M    2%
query57           1.02    1.10  0.93x    20     0      87M    8%
query58           0.61    0.54  1.13x    35     0      16M    2%
query59           1.39    1.33  1.05x    50     0     679M   66%
query60           0.91    0.82  1.10x    35     0      31M    3%
query61           0.91    1.05  0.87x    45     0      52M    5%
query62           0.87    0.87  0.99x    24     0     283M   28%
query63           0.60    0.58  1.05x    20     0     157M   15%
query64           7.14    6.61  1.08x    84     0     277M   27%
query65           2.54    2.54  1.00x    25     0     159M   16%
query66           1.97    1.99  0.99x    30     0     109M   11%
query67           7.47    7.66  0.98x    20     0     197M   19%
query68           1.06    1.30  0.82x    30     0      82M    8%
query69           1.01    0.97  1.04x    35     0      13M    1%
query70           1.11    1.14  0.98x    25     0     119M   12%
query71           2.70    2.69  1.01x    30     0      32M    3%
query72           2.09    1.93  1.08x    55     0     100M   10%
query73           0.88    0.85  1.04x    25     0      49M    5%
query74           1.70    1.65  1.03x    35     0      76M    7%
query75           3.90    3.81  1.02x    53     0      97M    9%
query76           1.32    1.33  0.99x    25     0     539M   53%
query77           0.81    0.74  1.09x    45     0      28M    3%
query78           6.50    6.65  0.98x    35     0      91M    9%
query79           0.74    0.78  0.96x    25     0     142M   14%
query80           2.60    2.55  1.02x    68     0      47M    5%
query81           1.73    1.84  0.94x    30     0      21M    2%
query82           0.53    0.63  0.85x    20     0     242M   24%
query83           0.56    0.48  1.17x    35     0       7M    1%
query84           0.86    0.67  1.30x    30     0      29M    3%
query85           1.11    1.06  1.05x    38     0      51M    5%
query86           0.70    0.68  1.02x    15     0      31M    3%
query87           1.38    1.34  1.03x    25     0      47M    5%
query88           2.91    2.89  1.01x    90     0     738M   72%
query89           0.86    0.80  1.08x    20     0     158M   15%
query90           0.49    0.51  0.97x    30     0     173M   17%
query91           0.78    0.86  0.91x    44     0      24M    2%
query92           0.44    0.44  1.01x    25     0       8M    1%
query93           9.36   10.11  0.93x    20     5     505M   49%
query94           1.72    1.76  0.98x    40     0     154M   15%
query95           3.55    3.49  1.02x    40     0     137M   13%
query96           1.08    1.27  0.85x    16     0     666M   65%
query97           1.50    1.49  1.00x    15     0      91M    9%
query98           0.92    0.88  1.04x    15     0      30M    3%
query99           1.20    1.19  1.01x    25     0     608M   59%
```

**Totals** (over 103 comparable queries): OFF warm 176.4 s → core1 warm 179.0 s = **0.99×**.
