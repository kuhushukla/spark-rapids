# Cross-dataset scan-component transfer

Status: executed and validated. Frozen verdict: **NOT SUPPORTED**. See [RESULTS.md](RESULTS.md).

This experiment tests whether the composable scan-history POC can transfer from
six yellow-taxi monthly scans to six untouched high-volume-for-hire monthly scans.
The full schemas and row populations differ. The query deliberately projects the
same physical shape from both tables: two nullable integers and one nullable
double.

The model is frozen in [manifest.yaml](manifest.yaml). It predicts:

- decoded GPU output bytes from Spark task input bytes;
- decoded rows from Spark task input bytes; and
- task GPU footprint from maximum output batch bytes.

Exact query/table identity is retained as provenance but is not used as a
prediction match.

## Storage discipline

All source data, Spark local directories, shuffle spill, event logs, and temporary
stdout belong under `/data`. The repository receives only scripts, manifests,
checksums, compressed evidence that remains reasonably small, and derived results.
Do not use the encrypted repository filesystem for Spark local or shuffle data.

## Frozen execution

The schedule is [schedule.json](schedule.json). The driver script is
[scripts/benchmark.py](scripts/benchmark.py). CPU and GPU runs use the same query
and schedule. GPU event logs are normalized with the previously versioned
`../uncompressed-size-shmoo/scripts/extract_scan_metrics.py`; analysis is performed
by [scripts/analyze.py](scripts/analyze.py).

The exact executed commands and environment are written after execution under
`provenance/`. The external raw run directory is retained under
`/data/tmp/cross-dataset-scan-transfer-<run-id>/`.

## Interpretation

Passing supports only the declared component transfer:

- yellow to high-volume-for-hire;
- the common projected physical schema;
- local Parquet/Snappy and the recorded RAPIDS build;
- the recorded batch and reader mechanisms.

The frozen model failed its median decoded-row/byte and footprint-upper-coverage
gates. The post-hoc decomposition identifies a transferable decoded-width
component and a non-transferable rows-per-input-byte component without changing
the frozen verdict. See [RESULTS.md](RESULTS.md).
