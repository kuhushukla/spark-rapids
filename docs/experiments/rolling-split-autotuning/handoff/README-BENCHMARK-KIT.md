# Scan-split and partition-count benchmark kit

Measures two heuristics against a swept baseline: the scan-split autotuner (`strategy=ratio`) and the
shuffle partition-count rule. No AI in the loop.

## Files (everything else in this directory belongs to older experiments)

| file | role |
|---|---|
| `run_scan_bench.sh` | runner, all stages |
| `bench_clickstream.scala` | cs01, cs02, cs03, csH, csH3 |
| `bench_pageviews.scala` | pv02, pv03g, pv05g, pv06, pv09g, pvH, pvU1, pvU2 |
| `bench_overture.scala` | gf1-3, hs1-3, rw6-9 |
| `partition_rule_full.py` | the partition rule; run standalone for a decision, `--json` for machines |
| `gen_ratio_report.py` | shared event-log parsing |
| `gen_partition_rule_report.py` | shared metric extraction |
| `gen_clean_2x2_report.py` | the report |
| `download_{clickstream,pageviews,overture}.sh` | data |

No source dependency: this directory is self-contained and can live in its own repo.

## Plugin jar

Supply a built RAPIDS jar matching your Spark version, by flag or environment:

```
export RAPIDS_JAR=/path/to/rapids-4-spark_2.12-<ver>-cuda12.jar   # or --jar=PATH
```
To build one from a spark-rapids checkout: `mvn -Dbuildver=353 -DskipTests package -pl dist -am`
(use the buildver matching your cluster's Spark).

## Query status

All queries carry their analytical question in the file header. Two are not usable:

- `pv07g` DOES NOT RUN. Crashes on iteration 2 in `RapidsShuffleThreadedWriter.doCommitAllPartitions`,
  then the JVM aborts.
- `ovJ1` NEVER RUN. Added as an expand-bucket candidate; its exchange size has not been probed.

## Run one query end to end

```
Q=cs03; DATA=/data/wiki-clickstream/parquet; OUT=/data/myrun
A="--out=$OUT --iters=5 --queries=$Q --data=$DATA --bench=$PWD/bench_clickstream.scala"
./run_scan_bench.sh $A --only=off      # 5-point split sweep, sets the baseline
./run_scan_bench.sh $A --only=ratio    # split autotuner arm
./run_scan_bench.sh $A --only=parts    # 2x2: baseline, rule partitions, and both
python3 gen_clean_2x2_report.py --run-dir=$OUT --queries=$Q --force
```

Flags: `--jar`, `--spark-home`, `--gpu` (UUID), `--localdir`, `--sweep`, `--aqe=on|off`,
`--off-iters` (accept a shorter sweep), `--nsys`, `--nsys-cpu`.

## Rules that keep results honest

- The baseline split is always the sweep winner. The `parts` stage refuses to run without a completed
  sweep rather than substituting a default.
- Iteration 1 is COLD_START and is dropped. Reports use the median of the warm iterations and print
  every warm iteration.
- Each arm is its own Spark session and event log. The runner skips arms that already have enough
  iterations, so a re-invocation resumes.
- Partition counts are derived rolling: each arm reads the rule's decision from its own prior run.
- Time columns are elapsed time on a shared GPU, not work. Read the byte columns for work.

## Known limits

- Queries writing more than about 50 GiB per arm are not repeatable on a single NVMe. Sustained write
  speed falls from 4.9 to 1.2 GB/s past the drive's SLC cache, so later arms are penalised. Measured:
  two identical cs01 arms gave 244.7 and 297.1 seconds.
- cs01 at a 4g split dies with RMM pool exhaustion.
- No query here exercises parquet schema evolution; all files in each dataset share one schema and
  `mergeSchema` is never set.
