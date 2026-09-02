# Clickstream csH3 / cs02: what actually differs between the two queries

**Date:** 2026-08-21. **Dataset:** `/data/wiki-clickstream/parquet`, 137 GiB, 344 files.

> **Measured against the table-only history key (pre-2026-09).** This document characterises the
> DATA -- read selectivity, compression, expansion ratio -- so its findings are independent of
> how history is keyed. The `ScanSplitAutotuner` reference below names a class the current
> plugin no longer has; the equivalent is `ScanSplitHeuristic`. See `README.md` §1.

Why this doc: csH3 and cs02 read the **same table with no filters**, yet the scan-split autotuner sizes
them differently (328M vs 528M). This records exactly what differs, measured, so the cross-query
learning result can be read against a known cause rather than a guess.

---

## 1. Source event logs

One matched pair, both at an identical **256m** split, so every difference below is the query and not
the split:

| query | arm | event log |
|---|---|---|
| csH3 | off-256m | `/data/cs-sweep-20260821/csH3-off-256m/el/local-1787322623470` |
| cs02 | off-256m | `/data/cs-sweep-20260821/cs02-off-256m/el/local-1787324308204` |

Both from the 2026-08-21 sweep (5 iterations/arm, `spark.rapids.filecache.enabled=false`). Report:
`docs/experiments/rolling-split-autotuning/results/cs-sweep-20260821-report.html`, manifest
`handoff/cs-sweep-20260821-manifest.json`.

## 2. Column pruning and filtering

Read from `sparkPlanInfo` metadata (`ReadSchema`, `PushedFilters`, `DataFilters`, `PartitionFilters`)
of every `Scan` node in the first execution of each log.

| | csH3 | cs02 |
|---|---|---|
| scan nodes | **3** (one per `UNION ALL` branch, all identical) | 1 |
| ReadSchema | `previous, current, link_type, n` | `previous, link_type, n` |
| columns read | 4 of 4 | 3 of 4 — drops `current` |
| PushedFilters | `[]` | `[]` |
| DataFilters | `[]` | `[]` |
| PartitionFilters | `[]` | `[]` |

**Filtering is not a difference: neither query filters anything.** No predicate is pushed, no row group
is skipped, both read every row. The whole difference is column pruning, and it is exactly one column.

## 3. Compression

Codec is **Snappy**, established three ways: every file is named `*.snappy.parquet`; every column chunk
in the footer reports `compression=SNAPPY`; and `handoff/download_clickstream.sh:70` writes with a bare
`.write.mode("overwrite").parquet(...)`, so Spark's default `spark.sql.parquet.compression.codec=snappy`
applied. No mixed codecs.

Footer statistics, 10 of 344 files sampled (215,098,351 rows):

| column | compressed GiB | uncompressed GiB | ratio | share of compressed |
|---|---|---|---|---|
| previous | 1.968 | 4.165 | 2.12x | 47.8% |
| current | 1.862 | 4.997 | 2.68x | 45.2% |
| n | 0.254 | 0.274 | 1.08x | 6.2% |
| link_type | 0.036 | 0.039 | 1.09x | 0.9% |
| **total** | **4.120** | **9.475** | **2.30x** | |

About **2.3x overall**, carried entirely by the two string columns. `n` (bigint) and `link_type` are
effectively incompressible at 1.08-1.09x; `link_type` has few distinct values but is already tiny after
dictionary encoding.

## 4. How one column becomes a different split

```
same table, same rows, no filters
        |
        +-- csH3 reads 4 of 4 columns   -> read_selectivity 0.9996
        +-- cs02 reads 3 of 4 (no `current`) -> read_selectivity 0.5688
                    |
        expansion_ratio = read_selectivity x decode_expansion
                    csH3: 0.9996 x 3.1274 = 3.1260
                    cs02: 0.5688 x 3.4094 = 1.9394
                    |
        split = batchSizeBytes / expansion_ratio   (batchSizeBytes = 1 GiB)
                    csH3: 1024 / 3.126 = 327.6 MiB   (recorded 343,485,760 B)
                    cs02: 1024 / 1.939 = 528   MiB   (recorded 553,658,427 B)
```

Both queries decode roughly the same amount per byte read (3.13x vs 3.41x). The split difference comes
from how many bytes they read, not from how those bytes decode.

## 5. Cross-checks

The share of bytes `current` represents is confirmed by three independent sources:

| source | cs02's share of the table's compressed bytes |
|---|---|
| parquet footer arithmetic (§3) | 54.80% |
| plugin's recorded `read_selectivity` (DECIDED line) | 56.88% |
| Spark input metrics (77.7 GiB / 135.50 GiB listed) | 57.3% |

Within ~2.5% of each other.

Separately, csH3's three scan nodes explain what otherwise looks inconsistent: the event log's total
`sum of output GPU batch bytes` is 1270.8 GiB/iteration, but the autotuner records **per scan node**, so
the ratio it stores is 1270.8/3 = 423.6 GiB over 135.50 GiB listed = 3.126 — not 9.38.

## 6. Not verified

- The 2.30x parquet compression ratio is **not** the same quantity as `decode_expansion` (3.13-3.41x).
  Decode expansion is GPU batch bytes over bytes read, and a cuDF string column materialises as offsets
  plus characters, which is larger than parquet's uncompressed representation. The gap is that
  representation difference plus dictionary/RLE decoding. I have not decomposed it, so do not quote a
  split between those two effects.
- Footer stats are a 10-file sample, not all 344 files.
- Whether the three csH3 branch records differ from one another in practice (they are schema-identical,
  but each appends its own record) has not been measured.

## 7. Why this matters for cross-query learning

`ScanSplitAutotuner.tableLabel` (`ScanSplitAutotuner.scala:304`) is the catalog name or root path, and
`latestFor` (lines 130-131) filters on that label alone and takes `lastOption`. Both queries therefore
share one history slot under `file:/data/wiki-clickstream/parquet`.

The record stores `listedBytes` and `decodedBytes` — it carries no trace of which columns produced them.
So a record written by a 4-column reader and a record written by a 3-column reader are indistinguishable
to the key, even though, as measured above, they imply splits 1.7x apart.

## 8. Reproducing

```bash
# column pruning + filters: read ReadSchema/PushedFilters/DataFilters from the plan in each event log
# compression: pyarrow over the parquet footers
python3 -c "
import pyarrow.parquet as pq, glob, collections
files=sorted(glob.glob('/data/wiki-clickstream/parquet/*.parquet'))
comp=collections.defaultdict(int); uncomp=collections.defaultdict(int)
for f in files[::35][:10]:
    md=pq.ParquetFile(f).metadata
    for rg in range(md.num_row_groups):
        g=md.row_group(rg)
        for c in range(g.num_columns):
            col=g.column(c)
            comp[col.path_in_schema]+=col.total_compressed_size
            uncomp[col.path_in_schema]+=col.total_uncompressed_size
print({k:(comp[k],uncomp[k],round(uncomp[k]/comp[k],2)) for k in comp})"
```

The `DECIDED` lines carrying `expansion_ratio`, `read_selectivity`, `decode_expansion` and `split_bytes`
are in `/data/cs-sweep-20260821/{csH3,cs02}-ftt-ratio/run.log`.
