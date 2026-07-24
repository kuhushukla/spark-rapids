# NDS SF3k — listed vs 2g, split by scan dominance (2026-07-21)

`scan_dominance = scanstage_gpu_s / wholequery_gpu_s`, measured on the **2g run** (`data/mpb-perquery-2g.csv`): the fraction of the query's GPU time spent in scan-containing stages. Buckets: scan-dominated >=0.8, mixed 0.4-0.8, downstream-heavy <0.4. Columns and measurement are defined in `nds-sf3k-listed-vs-2g-20260721.md`. `warm` is wall-clock ms; `gpuTime`/`scan` are aggregate task-seconds per warm iteration.

## Summary by bucket

| bucket | queries | WARM 2g->listed | gpuTime 2g->listed | queries faster | goal met (fuller+lower gpu+runtime<=+5%) |
|---|---|---|---|---|---|
| scan-dominated | 61 | 68.2->74.8s (+10%) | 1819->1023s (-44%) | 33/61 | 39/61 |
| mixed | 30 | 60.7->89.6s (+48%) | 1396->1037s (-26%) | 1/30 | 2/30 |
| downstream-heavy | 12 | 44.0->51.5s (+17%) | 1634->1627s (-0%) | 1/12 | 1/12 |

## Best run-1 signal and the gated policy

Of 103 queries, **44 keep runtime within +5% under listed** (call them winners). Testing several run-1 signals (all measured on the 2g run) for how well they separate winners from regressors, `scan_dominance` is the best single separator (~83% at threshold >= 0.96); batch fullness / avg batch / GPU-intensity were 63-69%.

**Gated policy**: apply the ratio only when `scan_dominance >= 0.96`, else keep the 2g split. It applies to 38 queries (32/38 stay within +5%), giving:

| policy | WARM runtime | gpuTime |
|---|---|---|
| all 2g (baseline) | 172.9 s | 4849 |
| all listed (blind ratio) | 215.9 s (+25%) | 3687 |
| gated (scan_dominance >= 0.96) | 171.0 s (-1.1%) | 4311 (-11%) |

So gating on the one run-1 signal gives gpuTime down ~11% with runtime within budget (~-1%), vs the blind ratio's +25% runtime. Caveats: ~6 of the applied queries still misfire, the gate captures about half the achievable gpuTime win, and 0.96 is fit to this NDS run (should be tunable).


## scan-dominated (61 queries)

| query | scan_dom | warm_2g | warm_listed | speedup | gpuTime_2g | gpuTime_listed | full%_2g | full%_listed |
|---|---|---|---|---|---|---|---|---|
| query77 | 0.98 | 771 | 523 | 1.47x | 5.8 | 0.4 | 2 | 44 |
| query66 | 1.00 | 1964 | 1392 | 1.41x | 111.4 | 15.8 | 11 | 82 |
| query52 | 1.00 | 360 | 262 | 1.37x | 2.5 | 0.2 | 3 | 53 |
| query53 | 1.00 | 521 | 388 | 1.34x | 8.1 | 2.5 | 16 | 84 |
| query89 | 0.99 | 780 | 604 | 1.29x | 16.4 | 5.4 | 16 | 84 |
| query43 | 1.00 | 584 | 462 | 1.26x | 13.6 | 3.5 | 12 | 82 |
| query37 | 0.95 | 441 | 353 | 1.25x | 6.1 | 2.0 | 10 | 42 |
| query47 | 0.99 | 1342 | 1093 | 1.23x | 38.4 | 11.3 | 17 | 86 |
| query3 | 0.98 | 353 | 288 | 1.23x | 6.0 | 0.9 | 10 | 80 |
| query55 | 0.96 | 332 | 274 | 1.21x | 2.4 | 0.2 | 3 | 53 |
| query32 | 1.00 | 605 | 501 | 1.21x | 3.6 | 0.2 | 2 | 47 |
| query71 | 0.98 | 2625 | 2182 | 1.20x | 14.5 | 0.6 | 3 | 58 |
| query48 | 0.99 | 645 | 568 | 1.14x | 13.4 | 4.5 | 28 | 84 |
| query63 | 1.00 | 556 | 490 | 1.13x | 8.6 | 2.7 | 16 | 84 |
| query83 | 0.93 | 536 | 474 | 1.13x | 1.5 | 0.1 | 0 | 2 |
| query27 | 0.98 | 697 | 618 | 1.13x | 25.1 | 6.5 | 32 | 83 |
| query90 | 1.00 | 519 | 468 | 1.11x | 9.4 | 3.2 | 19 | 86 |
| query92 | 1.00 | 437 | 397 | 1.10x | 3.1 | 0.1 | 1 | 35 |
| query59 | 1.00 | 1356 | 1234 | 1.10x | 45.3 | 31.6 | 48 | 91 |
| query26 | 0.98 | 542 | 498 | 1.09x | 13.3 | 2.8 | 16 | 80 |
| query57 | 0.98 | 974 | 898 | 1.08x | 20.3 | 3.8 | 9 | 80 |
| query61 | 0.92 | 1067 | 988 | 1.08x | 7.9 | 1.1 | 5 | 64 |
| query12 | 0.86 | 414 | 384 | 1.08x | 0.7 | 0.1 | 1 | 10 |
| query2 | 1.00 | 977 | 913 | 1.07x | 35.7 | 12.1 | 17 | 84 |
| query70 | 1.00 | 1070 | 1000 | 1.07x | 35.5 | 8.4 | 12 | 88 |
| query9 | 1.00 | 1425 | 1354 | 1.05x | 57.4 | 47.2 | 49 | 88 |
| query98 | 1.00 | 909 | 864 | 1.05x | 0.7 | 0.1 | 3 | 32 |
| query28 | 1.00 | 3704 | 3586 | 1.03x | 203.8 | 175.8 | 66 | 87 |
| query31 | 0.87 | 1367 | 1328 | 1.03x | 20.6 | 2.2 | 2 | 67 |
| query58 | 1.00 | 535 | 520 | 1.03x | 0.7 | 0.2 | 2 | 6 |
| query88 | 1.00 | 2835 | 2769 | 1.02x | 179.8 | 158.9 | 49 | 90 |
| query99 | 1.00 | 1148 | 1122 | 1.02x | 57.9 | 33.1 | 52 | 89 |
| query96 | 1.00 | 1035 | 1032 | 1.00x | 22.3 | 16.0 | 49 | 88 |
| query13 | 0.99 | 836 | 836 | 1.00x | 14.3 | 9.3 | 39 | 78 |
| query16 | 1.00 | 1075 | 1090 | 0.99x | 28.4 | 23.6 | 34 | 66 |
| query86 | 0.99 | 643 | 652 | 0.99x | 17.4 | 1.1 | 3 | 62 |
| query36 | 1.00 | 814 | 834 | 0.98x | 34.0 | 5.4 | 20 | 83 |
| query42 | 1.00 | 267 | 274 | 0.97x | 2.5 | 0.2 | 3 | 50 |
| query20 | 0.86 | 380 | 392 | 0.97x | 0.7 | 0.1 | 2 | 18 |
| query23_part1 | 0.88 | 4804 | 5072 | 0.95x | 261.2 | 209.5 | 35 | 60 |
| query76 | 1.00 | 1289 | 1403 | 0.92x | 64.6 | 35.4 | 47 | 93 |
| query54 | 0.88 | 1042 | 1135 | 0.92x | 7.6 | 1.4 | 3 | 45 |
| query14_part2 | 0.90 | 3956 | 4382 | 0.90x | 93.4 | 49.7 | 14 | 59 |
| query56 | 0.93 | 638 | 720 | 0.89x | 2.9 | 0.3 | 2 | 28 |
| query62 | 1.00 | 814 | 922 | 0.88x | 32.4 | 14.0 | 30 | 84 |
| query7 | 0.98 | 858 | 976 | 0.88x | 25.1 | 9.3 | 32 | 82 |
| query14_part1 | 0.88 | 4184 | 4791 | 0.87x | 98.0 | 53.1 | 11 | 62 |
| query8 | 0.98 | 657 | 796 | 0.83x | 4.1 | 0.5 | 3 | 41 |
| query82 | 0.95 | 552 | 720 | 0.77x | 12.9 | 3.8 | 16 | 66 |
| query5 | 1.00 | 1987 | 2680 | 0.74x | 41.3 | 32.3 | 10 | 58 |
| query91 | 0.86 | 738 | 1047 | 0.70x | 1.4 | 0.2 | 1 | 10 |
| query60 | 0.85 | 773 | 1106 | 0.70x | 6.5 | 1.5 | 3 | 50 |
| query44 | 1.00 | 317 | 455 | 0.70x | 0.0 | 0.0 | 1 | 1 |
| query19 | 0.84 | 729 | 1067 | 0.68x | 4.4 | 0.9 | 4 | 50 |
| query34 | 0.94 | 1494 | 2468 | 0.61x | 8.3 | 2.3 | 13 | 76 |
| query68 | 0.96 | 991 | 1683 | 0.59x | 6.7 | 1.2 | 8 | 66 |
| query49 | 0.89 | 1294 | 2245 | 0.58x | 17.6 | 3.2 | 5 | 72 |
| query79 | 0.85 | 727 | 1314 | 0.55x | 10.7 | 3.7 | 14 | 77 |
| query46 | 0.95 | 1024 | 1900 | 0.54x | 15.0 | 4.8 | 23 | 81 |
| query73 | 0.92 | 888 | 1880 | 0.47x | 5.1 | 1.1 | 5 | 54 |
| query1 | 0.87 | 1018 | 2179 | 0.47x | 10.3 | 1.7 | 2 | 64 |

## mixed (30 queries)

| query | scan_dom | warm_2g | warm_listed | speedup | gpuTime_2g | gpuTime_listed | full%_2g | full%_listed |
|---|---|---|---|---|---|---|---|---|
| query21 | 0.75 | 343 | 280 | 1.23x | 0.4 | 0.1 | 5 | 14 |
| query23_part2 | 0.77 | 5390 | 5546 | 0.97x | 298.1 | 244.1 | 34 | 60 |
| query18 | 0.49 | 1553 | 1684 | 0.92x | 14.8 | 9.8 | 17 | 81 |
| query64 | 0.51 | 6892 | 7697 | 0.90x | 110.5 | 122.9 | 29 | 84 |
| query35 | 0.70 | 1200 | 1430 | 0.84x | 23.9 | 7.1 | 2 | 68 |
| query4 | 0.66 | 3311 | 4002 | 0.83x | 132.3 | 72.8 | 14 | 89 |
| query85 | 0.80 | 883 | 1094 | 0.81x | 13.3 | 2.8 | 6 | 62 |
| query51 | 0.70 | 1410 | 1824 | 0.77x | 15.5 | 6.4 | 8 | 88 |
| query10 | 0.59 | 1062 | 1432 | 0.74x | 15.3 | 5.4 | 1 | 50 |
| query65 | 0.49 | 2678 | 3639 | 0.74x | 45.3 | 38.8 | 16 | 88 |
| query75 | 0.57 | 3726 | 5087 | 0.73x | 86.3 | 48.8 | 10 | 85 |
| query69 | 0.70 | 951 | 1330 | 0.72x | 9.3 | 3.2 | 1 | 43 |
| query24_part1 | 0.66 | 5690 | 8111 | 0.70x | 138.7 | 140.2 | 48 | 86 |
| query24_part2 | 0.67 | 5757 | 8490 | 0.68x | 143.4 | 141.1 | 48 | 86 |
| query74 | 0.66 | 1611 | 2449 | 0.66x | 36.7 | 14.2 | 8 | 85 |
| query72 | 0.52 | 1883 | 2914 | 0.65x | 44.2 | 30.3 | 11 | 72 |
| query11 | 0.55 | 2006 | 3128 | 0.64x | 59.3 | 31.1 | 10 | 89 |
| query87 | 0.57 | 1337 | 2117 | 0.63x | 27.8 | 11.7 | 5 | 85 |
| query38 | 0.56 | 1415 | 2342 | 0.60x | 29.3 | 12.2 | 5 | 82 |
| query97 | 0.47 | 1536 | 2679 | 0.57x | 44.1 | 34.8 | 9 | 89 |
| query17 | 0.49 | 1259 | 2272 | 0.55x | 24.1 | 13.2 | 3 | 68 |
| query81 | 0.60 | 1586 | 2916 | 0.54x | 14.2 | 6.0 | 2 | 50 |
| query30 | 0.54 | 1340 | 2520 | 0.53x | 14.4 | 7.1 | 2 | 52 |
| query15 | 0.50 | 872 | 1640 | 0.53x | 5.4 | 2.6 | 2 | 38 |
| query45 | 0.57 | 847 | 1817 | 0.47x | 4.4 | 2.1 | 1 | 22 |
| query25 | 0.47 | 1089 | 2684 | 0.41x | 21.2 | 15.9 | 3 | 64 |
| query33 | 0.74 | 754 | 1963 | 0.38x | 4.2 | 1.9 | 2 | 28 |
| query84 | 0.71 | 718 | 1937 | 0.37x | 6.2 | 3.0 | 3 | 39 |
| query6 | 0.45 | 721 | 2004 | 0.36x | 2.2 | 1.4 | 3 | 21 |
| query40 | 0.44 | 864 | 2526 | 0.34x | 11.4 | 6.2 | 3 | 55 |

## downstream-heavy (12 queries)

| query | scan_dom | warm_2g | warm_listed | speedup | gpuTime_2g | gpuTime_listed | full%_2g | full%_listed |
|---|---|---|---|---|---|---|---|---|
| query39_part1 | 0.25 | 1099 | 1030 | 1.07x | 1.6 | 0.4 | 4 | 8 |
| query93 | 0.32 | 9588 | 9800 | 0.98x | 353.5 | 384.0 | 52 | 87 |
| query41 | 0.00 | 214 | 221 | 0.97x | 0.1 | 0.0 | 2 | 2 |
| query50 | 0.27 | 6868 | 7279 | 0.94x | 261.0 | 299.4 | 69 | 89 |
| query39_part2 | 0.27 | 819 | 888 | 0.92x | 1.5 | 0.3 | 4 | 8 |
| query67 | 0.05 | 7950 | 9129 | 0.87x | 397.4 | 378.4 | 20 | 83 |
| query78 | 0.24 | 6553 | 7775 | 0.84x | 254.4 | 252.6 | 10 | 86 |
| query95 | 0.09 | 3352 | 4194 | 0.80x | 182.4 | 176.5 | 17 | 71 |
| query29 | 0.34 | 2422 | 3265 | 0.74x | 59.4 | 47.3 | 14 | 83 |
| query94 | 0.25 | 1713 | 2376 | 0.72x | 62.7 | 51.5 | 19 | 68 |
| query80 | 0.32 | 2445 | 3942 | 0.62x | 51.9 | 28.9 | 6 | 65 |
| query22 | 0.17 | 934 | 1640 | 0.57x | 8.4 | 7.5 | 4 | 48 |
