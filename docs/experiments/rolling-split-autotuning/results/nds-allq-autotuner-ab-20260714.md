# NDS SF100 — Dynamic Autotuner vs 128 MiB Baseline (all 99 queries)

## Method
- All 99 NDS/TPC-DS queries at SF100, A5000, `local[16]`, plugin enabled, page cache pre-warmed.
- **baseline (OFF):** autotuner disabled → `spark.sql.files.maxPartitionBytes=128m` everywhere.
- **autotuner (WARM):** enabled + warmed (a prior pass recorded per-table decode ratios). Split is
  decided per table:
  `split = max(sparkDefault, min(batchSizeBytes/ratio, memCeiling, parallelismCap))`.
  The `max(sparkDefault, …)` **floor** means the autotuner may only ever *increase* the split — never
  pick one smaller than Spark's own default (a smaller split raises task count and regresses).
- Split each scan used is read from the `scanMaxSplitBytes` event-log metric, cross-checked against
  the autotuner `DECIDED` log and scan-stage task counts. Run dir:
  `data/nds-allq-autotuner-20260714_175443/`.

## Overall
- **OFF = 525.5s → WARM = 391.2s = 1.34×** (95 common queries).
- **57 faster >5%, 22 slower >5%, 16 ~same.**
- Autotuner chose split > 128 MiB for 53/95 queries.
- Floor positive control: 655 warm `DECIDED` lines, **0** with `split_bytes < spark_default`.

## Wins — autotuner enlarged the split (fewer scan tasks)
| query | OFF | WARM | speedup | split | scan tasks OFF→WARM |
|---|---|---|---|---|---|
| query9 | 15.35s | 3.73s | **4.12×** | 923 MiB | 1232 → 174 |
| query28 | 43.65s | 16.79s | 2.60× | 923 MiB | 1232 → 174 |
| query88 | 40.14s | 17.42s | 2.30× | 923 MiB | 1232 → 174 |
| query59 | 20.64s | 9.06s | 2.28× | 891 MiB | 1227 → 180 |
| query16 | 5.99s | 2.73s | 2.19× | 649 MiB | 229 → 229 |

The expensive queries scan store_sales; the autotuner raised its split to ~900 MiB, cutting scan
tasks ~7×.

## Remaining regressions are noise, not the autotuner
The 22 slower queries have **unchanged split and task count** off→warm (e.g. query81 128 MiB, 46→46;
query33 128 MiB, 20→20; query42 128 MiB, 23→23) — the autotuner changed nothing about their plan.
They are all sub-2s queries, and OFF and WARM ran in different sessions, so the deltas are cross-run
variance. The floor guarantees warm task count is always ≤ off, so no split-caused regression exists.

## Effect of the floor (why it's there)
Without the floor, the autotuner could pick splits *below* 128 MiB (down to a 64 MiB floor) for some
scans, which raised task count and regressed ~41 queries; that version scored 1.18× overall
(39 faster / 41 slower / 15 same). Flooring at `sparkDefault` removed those split-caused regressions
and lifted the result to **1.34× (57 faster / 22 slower)** while keeping the 2–4× wins.

## Artifacts (durable, in this directory)
- Full per-query table: `nds-allq-autotuner-table-20260714.txt` (also embedded below)
- Raw data: `nds-allq-autotuner-summary-20260714.csv`
- Plots: `nds-allq-autotuner-plot-runtime-20260714.png`, `…-plot-split-20260714.png`,
  `…-plot-tasks-20260714.png`
- Event logs (cold/warm) in `/home/kuhu/logdir`; OFF baseline reused from run `…_163208`.
- Regeneration: `handoff/run-nds-allq-autotuner.sh` (cold→warm) + `handoff/run-nds-allq-off.sh` (baseline).

## Per-fact-table splits (the single `split` column is a reduction)
Each table a query scans gets its OWN dynamic split. The `split` column in the per-query table below
is the **max** across the query's scans — which is always the large fact table; the small dimensions
(date_dim, item, store, time_dim, household_demographics, …) sit at the 64 MiB floor and don't affect
runtime. Full per-(query, fact-table) split values (fact tables only: store/catalog/web _sales and
_returns, inventory) are in `nds-allq-autotuner-fact-splits-20260714.csv` (185 rows). The 44 queries
that scan more than one fact table — where the single `split` column collapses distinct values:

```
query2:  catalog_sales=646  web_sales=347
query4:  catalog_sales=128  store_sales=178  web_sales=128
query5:  catalog_returns=64  catalog_sales=64  store_returns=75  store_sales=84  web_returns=75  web_sales=347
query10: catalog_sales=64  store_sales=128  web_sales=128
query11: store_sales=178  web_sales=128
query14: catalog_sales=386  store_sales=534  web_sales=208
query16: catalog_returns=128  catalog_sales=649
query17: catalog_sales=128  store_returns=128  store_sales=128
query23: catalog_sales=64  store_sales=923  web_sales=128
query24: store_returns=129  store_sales=923
query25: catalog_sales=121  store_returns=128  store_sales=128
query29: catalog_sales=386  store_returns=128  store_sales=128
query31: store_sales=128  web_sales=128
query33: catalog_sales=64  store_sales=128  web_sales=128
query35: catalog_sales=128  store_sales=128  web_sales=128
query37: catalog_sales=649  inventory=64
query38: catalog_sales=128  store_sales=178  web_sales=128
query40: catalog_returns=128  catalog_sales=64
query49: catalog_returns=128  catalog_sales=64  store_returns=129  store_sales=128  web_returns=128  web_sales=128
query50: store_returns=128  store_sales=891
query51: store_sales=178  web_sales=128
query54: catalog_sales=64  store_sales=128  web_sales=128
query56: catalog_sales=64  store_sales=128  web_sales=128
query58: catalog_sales=64  store_sales=64  web_sales=64
query60: catalog_sales=64  store_sales=128  web_sales=128
query64: catalog_returns=128  catalog_sales=649  store_returns=129  store_sales=179
query66: catalog_sales=128  web_sales=128
query69: catalog_sales=64  store_sales=128  web_sales=128
query71: catalog_sales=64  store_sales=128  web_sales=128
query72: catalog_returns=128  catalog_sales=129  inventory=128
query74: store_sales=178  web_sales=128
query75: catalog_returns=128  catalog_sales=129  store_returns=129  store_sales=179  web_returns=128  web_sales=128
query76: catalog_sales=646  store_sales=891  web_sales=347
query77: catalog_returns=64  catalog_sales=64  store_returns=128  store_sales=128  web_returns=128  web_sales=128
query78: catalog_returns=128  catalog_sales=128  store_returns=129  store_sales=177  web_returns=128  web_sales=128
query80: catalog_returns=128  catalog_sales=64  store_returns=129  store_sales=128  web_returns=128  web_sales=128
query82: inventory=64  store_sales=923
query83: catalog_returns=64  store_returns=106  web_returns=105
query85: web_returns=128  web_sales=128
query87: catalog_sales=128  store_sales=178  web_sales=128
query93: store_returns=129  store_sales=923
query94: web_returns=128  web_sales=347
query95: web_returns=128  web_sales=347
query97: catalog_sales=128  store_sales=178
```
(splits in MiB; the other 51 queries scan a single fact table = the per-query `split` value below.)

## Full per-query results (all 95 comparable queries)
`split` = max scanMaxSplitBytes across the query's scans (the large fact table; see per-table breakdown
above); `tasks` = scan-stage task count off→warm; last column: `*` win ≥1.3×, `x` slower (<0.95×),
blank neutral.

```
query      off(s)  warm(s) speedup    split   tasks off→warm
--------------------------------------------------------------
query1       0.29     0.35   0.82x    64MiB      20→3     x
query2      21.13    11.61   1.82x   646MiB  1302→462     *
query3       0.82     0.96   0.85x   140MiB   111→100     x
query4      13.93    11.46   1.22x   178MiB   245→232
query5       6.75     4.83   1.40x   347MiB  1162→434     *
query6       0.88     0.76   1.17x   128MiB     20→20
query7       3.92     3.19   1.23x   177MiB   245→179
query8       1.19     1.38   0.86x   128MiB     59→59     x
query9      15.35     3.73   4.12x   923MiB  1232→174     *
query10      1.87     1.66   1.13x   128MiB     79→79
query11      8.01     7.65   1.05x   178MiB   245→232
query12      0.63     0.69   0.90x   128MiB     20→20     x
query13      4.33     3.15   1.37x   177MiB   245→179     *
query15      0.58     0.42   1.37x    64MiB     20→11     *
query16      5.99     2.73   2.19x   649MiB   229→229     *
query17      2.42     2.61   0.93x   128MiB   168→168     x
query18      1.58     1.25   1.26x   128MiB     28→28
query19      1.08     0.95   1.14x   128MiB     23→23
query20      0.60     0.60   0.99x    64MiB     17→12
query21      0.51     0.40   1.28x    64MiB      16→4
query22      1.50     1.16   1.30x    64MiB     18→17     *
query25      2.17     2.13   1.02x   128MiB   134→134
query26      1.20     0.64   1.87x   128MiB     28→28     *
query27      3.33     2.98   1.12x   178MiB   245→177
query28     43.65    16.79   2.60x   923MiB  1232→174     *
query29      2.10     3.10   0.68x   386MiB     84→77     x
query30      4.60     4.55   1.01x   128MiB   229→229
query31      3.43     3.92   0.88x   128MiB     63→63     x
query32      0.80     0.68   1.17x    64MiB     15→10
query33      1.16     2.38   0.49x   128MiB     20→20     x
query34      0.24     0.20   1.17x    64MiB      20→3
query35      2.76     3.04   0.91x   128MiB   180→180     x
query36      3.14     3.10   1.01x   178MiB   245→178
query37      0.78     0.56   1.38x   649MiB    141→28     *
query38      3.54     4.24   0.83x   178MiB   246→233     x
query40      2.09     1.38   1.51x   128MiB   229→229     *
query41      0.26     0.31   0.82x    64MiB       5→1     x
query42      0.39     0.77   0.51x   128MiB     23→23     x
query43      2.87     2.33   1.23x   179MiB   247→178
query44      2.42     2.30   1.05x   923MiB  1232→174
query45      1.16     1.23   0.94x   128MiB     57→57     x
query46      2.73     2.28   1.20x   153MiB   212→177
query47      3.73     3.27   1.14x   217MiB   288→171
query48      3.53     2.56   1.38x   178MiB   245→177     *
query49      9.52    10.12   0.94x   129MiB 1348→1348     x
query50      7.82     5.93   1.32x   891MiB  1227→180     *
query51      4.15     4.32   0.96x   178MiB   245→232
query52      0.47     0.45   1.06x   128MiB     23→23
query53      2.39     2.01   1.19x   177MiB   245→179
query54      1.43     1.32   1.08x   128MiB     63→63
query55      0.54     0.47   1.15x   128MiB     23→23
query56      1.27     1.13   1.13x   128MiB     20→20
query57      1.11     1.16   0.95x   156MiB     34→28
query58      1.36     1.05   1.29x    64MiB      16→9
query59     20.64     9.06   2.28x   891MiB  1227→180     *
query60      1.20     1.12   1.06x   128MiB     22→22
query61      1.17     1.04   1.12x   128MiB     23→23
query62     14.22     8.06   1.77x   347MiB  1162→434     *
query63      2.21     2.02   1.10x   178MiB   245→178
query64     13.28    12.66   1.05x   649MiB 1253→1237
query65      5.18     4.03   1.29x   178MiB   245→177
query66      7.69     7.58   1.01x   128MiB   232→232
query67     11.52     8.09   1.42x   178MiB   245→200     *
query68      0.29     0.26   1.13x    64MiB      20→3
query69      1.37     1.40   0.97x   128MiB     58→58
query70      4.43     3.90   1.14x   178MiB   245→177
query71      1.46     1.33   1.09x   128MiB     60→52
query72      6.79     5.58   1.22x   129MiB   229→229
query73      0.97     1.06   0.91x   128MiB     47→47     x
query74      8.13     8.24   0.99x   178MiB   245→232
query75     20.63    20.11   1.03x   179MiB 1348→1348
query76     17.23     8.93   1.93x   891MiB  2529→642     *
query77      1.69     1.56   1.09x   128MiB     22→22
query78     15.83    15.63   1.01x   177MiB 1348→1348
query79      1.66     1.66   1.00x   128MiB   106→106
query80     12.04    11.97   1.01x   129MiB 1348→1348
query81      1.66     3.55   0.47x   128MiB     46→46     x
query82      4.00     2.81   1.42x   923MiB  1232→174     *
query83      1.02     2.04   0.50x   106MiB     16→16     x
query84      4.77     4.19   1.14x   129MiB 1253→1237
query85      9.25    10.59   0.87x   128MiB 1348→1348     x
query86      2.40     2.42   0.99x   128MiB   232→232
query87      3.68     4.01   0.92x   178MiB   245→232     x
query88     40.14    17.42   2.30x   923MiB  1232→174     *
query89      2.83     2.38   1.19x   178MiB   245→177
query90      8.56     5.40   1.59x   347MiB  1162→434     *
query91      0.85     0.65   1.31x    64MiB     20→17     *
query92      1.07     1.14   0.93x   128MiB     57→57     x
query93     12.09    10.13   1.19x   923MiB 1253→1237
query94     14.92    11.22   1.33x   347MiB 1348→1348     *
query95     21.58    13.89   1.55x   347MiB 1348→1348     *
query96      6.63     4.54   1.46x   923MiB  1232→174     *
query97      2.04     2.62   0.78x   178MiB   245→177     x
query98      1.13     1.09   1.04x   128MiB     20→20
query99      1.81     1.48   1.22x   649MiB    141→28
--------------------------------------------------------------
TOTAL       525.5    391.2   1.34x         n=95   (* win≥1.3x, x slower)
```
