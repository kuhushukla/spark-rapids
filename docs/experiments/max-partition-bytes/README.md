# Exploratory `maxPartitionBytes` study on NYC TLC Parquet

## Split verdict

The mechanically generated verdict is intentionally split:

- **Row-group/empty-task mechanism: SUPPORTED.** This file has one Parquet row group. Every tested setting retained one useful scan task; smaller settings created 1–11 empty tasks.
- **Performance effect: EXPLORATORY / INCONCLUSIVE.** Across seven randomized complete blocks, no candidate showed an observed median scan-stage improvement of at least 5% versus 128 MiB.

This is not confirmatory noninferiority, equivalence, or superiority evidence. The four paired bootstrap intervals below are exploratory and unadjusted for multiple comparisons.

| Setting | Planned / useful / empty tasks | Median scan stage | Exploratory paired mean vs. 128 MiB |
|---:|---:|---:|---:|
| 128 MiB | 1 / 1 / 0 | 193 ms | baseline |
| 64 MiB | 2 / 1 / 1 | 190 ms | -1.66%; unadjusted 95% bootstrap interval [-4.15%, +0.39%] |
| 32 MiB | 3 / 1 / 2 | 193 ms | -0.13%; unadjusted interval [-2.45%, +1.88%] |
| 16 MiB | 6 / 1 / 5 | 193 ms | -1.09%; unadjusted interval [-3.20%, +1.03%] |
| 8 MiB | 12 / 1 / 11 | 191 ms | -1.18%; unadjusted interval [-4.20%, +1.47%] |

The means and medians answer different exploratory summaries; neither licenses a performance ranking with seven blocks. The only bounded performance statement is that no observed median was at least 5% faster than 128 MiB.

## Protocol provenance

The final manifest and protocol were versioned retrospectively after execution. There was no immutable pre-run manifest commit or external registration, so this is not described as an auditable preregistration.

Two earlier mutable harness-development failures are non-auditable notes only. Their raw outputs were not retained and they support no result claim. The accepted evidence comes solely from run `20260703T131700Z`, executed by the final checksummed scripts.

## What the three skills contributed

The diagnose workflow traced the actuator instead of treating the configured value as the effective split:

```text
totalBytes = sum(file.length + openCostInBytes)
bytesPerCore = totalBytes / minPartitionNum
effectiveSplitBytes =
  min(maxPartitionBytes, max(openCostInBytes, bytesPerCore))
```

Spark 3.5 may later repack when `maxPartitionNum` is set. The experiment pins `openCostInBytes=1`, `minPartitionNum=1`, and leaves `maxPartitionNum` unset. A new read/query is constructed after setting each value because the configuration is consumed when file partitions are planned/materialized; an already materialized scan is not resized.

The model-design workflow kept the schedule honest. Spark file partitions are scheduling units; they are not GPU batches. Task/footer costs occur inside a parallel task schedule, and batch costs occur inside the GPU schedule. They are not globally serial `P*H` or `K*H` terms. Measured rates are calibrated estimates, not capacity lower bounds.

The experiment workflow produced separate CPU/GPU applications, a canonical CPU aggregate payload, per-run append-only journals, exact seeded allocation checks, plan/hash checks for warm-ups and measured runs, all-stage Task End Reason validation, raw logs, stdout/stderr, an external-timeout wrapper, and a replayed mechanical verdict.

## Dataset

- Official source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Snapshot: [January 2020 yellow taxi Parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet)
- Dictionary: [Yellow Taxi Trip Records](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- Exact path used: `/home/roberte/src/rapids-plugin-4-spark/yellow_tripdata_2020-01.parquet`
- Size: 93,562,858 bytes
- SHA-256: `0c32c1d5ef0d37ac3ff1a3f1880f247cc40165edf18626e5a86866296a4a5b93`
- Layout: one row group, 6,405,008 rows, 93,550,347 compressed column-chunk bytes, 170,542,631 uncompressed bytes.

TLC provides the file as an immediate public download, says the records were supplied by technology providers, and disclaims their accuracy. The file page does not name a conventional data license; this study does not invent one. The exact repository-root filename is protected by a tracked `.gitignore` rule and remains untracked.

## Query and execution

The query projects `PULocationID`, `payment_type`, `passenger_count`, and `trip_distance`; retains nonnegative distances and non-null pickup zones; then counts trips and sums integral passenger counts by pickup zone and payment type.

```text
driver: file/range planning
              |
              v
resource-constrained parallel scan task schedule
 task: footer/filter/read -> GPU queue -> GPU batch/filter/partial aggregate
              |
        shuffle barrier
              |
      final GPU aggregate -> collect
```

Settings held fixed:

- Spark 3.5.5, local[8], AQE disabled, 32 shuffle partitions;
- RAPIDS COALESCING Parquet reader;
- static `concurrentGpuTasks=2`;
- one RTX A6000;
- local warm filesystem cache after one preserved warm-up per value.

The exact CPU/RAM/OS/GPU/software environment is in [environment.txt](provenance/environment.txt).

## Correctness and audit checks

The validator confirmed:

- all 40 GPU executions, including five warm-ups, matched the preserved canonical CPU payload hash;
- every warm-up and measured plan contained a GPU scan;
- seeded allocation and all seven blocks were exact and complete;
- every Task End Reason in every relevant CPU and GPU stage was Success;
- no relevant task was failed, killed, missing, or extra;
- journal plans, compact outputs, plan artifacts, and hashes reconciled;
- the validator replay against compressed raw logs reproduced the verdict byte-for-byte.

The canonical CPU rows are preserved in [cpu-reference.json](analysis/cpu-reference.json). The split verdict is in [validated-analysis.json](analysis/validated-analysis.json).

## Reproduce with a unique run

From the repository root:

```bash
export SPARK_HOME=/home/roberte/src/spark_3.5.5
export RAPIDS_JAR=dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
docs/experiments/max-partition-bytes/run_experiment.sh
```

The wrapper:

- requires the exact repository-root dataset path and refuses run-directory reuse;
- applies external CPU/GPU timeouts;
- requires exactly one event log per application;
- preserves stdout, stderr, event logs, append-only journals, plans, canonical output, hardware/software provenance, and executed-code hashes;
- validates exact allocation, all result/plan hashes, and all relevant Task End Reasons;
- packages compressed raw evidence, replays the validator, compares outputs byte-for-byte, and verifies checksums before recording wrapper success.

Verify the versioned package from the repository root:

```bash
(cd docs/experiments/max-partition-bytes && sha256sum -c provenance/checksums.txt)
```

The append-only wrapper journal is finalized only after payload checksum verification, so it is intentionally outside that payload checksum list.

## Evidence layout

```text
max-partition-bytes/
├── README.md
├── manifest.yaml
├── model-card.md
├── run_experiment.sh
├── scripts/
├── analysis/
├── raw/
├── stdout/
└── provenance/
```

The dataset and tested assembly JAR are not committed. Their hashes and exact identities are recorded in the manifest and environment evidence.

## Next experiment

Use several files containing at least 8–16 independently useful, uneven row groups. Keep the journaled protocol, add a hold-out projection/selectivity, and verify that candidate limits produce distinct useful-task layouts before timing. If a performance claim is desired, independently version a confirmatory design with multiplicity and noninferiority/equivalence rules fixed before results.
