# Profiled GPU tuning POC experiment

Status: **EXECUTED (exploratory mechanism validation)**

This package profiles the scan stage used by the partition-size experiment and supports
the model in
[`docs/design/composable-gpu-scan-performance-model.md`](../../design/composable-gpu-scan-performance-model.md).

## Question and scope

Can separately measured read, decode, kernel, and overlap signals explain both sides of
the observed partition-size curve, and are those signals implementable as a
context-keyed performance history?

This run validates mechanisms on one local RTX A6000 context. It does not establish a
portable optimum, a cloud/object-store read model, or the preregistered 10% predictive
accuracy target.

## Frozen context

- hardware key: `local-rtx-a6000`; RTX A6000, 49,140 MiB; eight Spark local threads;
- Spark 3.5.5 and the branch's RAPIDS build;
- 2009 taxi common-column query, Snappy Parquet, warm/repeated local filesystem;
- query: two-column scan followed by partial/final GPU aggregate;
- `spark.rapids.sql.batchSizeBytes=1 GiB`;
- dynamic admission enabled, initial concurrency four;
- candidate `maxPartitionBytes`: 128 MiB, 2,048 MiB, 16,384 MiB;
- two warmups, then target scan stage 4 for every stage-gated run.

The schedule and acceptance language were committed before execution in
`PROFILE_PLAN.md` and `preregistration/`.

## What ran

### Nsight Systems

`scripts/run_nsys.sh` used the container's Nsight Systems 2025.2.1 binary at:

`/opt/nvidia/nsight-compute/2025.2.1/host/target-linux-x64/nsys`

CUDA, NVTX, OS-runtime, and 1-kHz GPU metrics were captured. The event log and
`Stage 4 Task ...` NVTX ranges delimit the target scan stage. Run:

```bash
scripts/run_nsys.sh 128
scripts/run_nsys.sh 2048
scripts/run_nsys.sh 16384
```

The committed `.nsys-rep` files are the authoritative raw profiles. Recreate the
derived SQLite files, then the summary:

```bash
NSYS=/opt/nvidia/nsight-compute/2025.2.1/host/target-linux-x64/nsys
$NSYS export --type sqlite --force-overwrite=true \
  --output attempts/nsys-128-001/analysis/profile.sqlite \
  attempts/nsys-128-001/raw/profile.nsys-rep
# repeat for 2048 and 16384
python3 scripts/analyze_nsys.py \
  --attempt attempts/nsys-128-001 \
  --attempt attempts/nsys-2048-001 \
  --attempt attempts/nsys-16384-001 \
  --output /tmp/nsys-summary.json --markdown /tmp/NSYS_RESULTS.md
cmp /tmp/nsys-summary.json analysis/nsys-summary.json
cmp /tmp/NSYS_RESULTS.md NSYS_RESULTS.md
```

The SQLite exports are excluded because they total roughly 177 MiB and are deterministic
derivatives of the reports.

### RAPIDS stage-gated CUPTI

`scripts/run_cupti.sh` ran the independent built-in profiler at the same three cells.
The raw `.bin` plus CRC files are retained. The expanded converter JSON is excluded
because the 128-MiB file alone is about 38 MiB and can be regenerated with:

```bash
/home/roberte/src/spark-rapids-jni/target/jni/cmake-build/profiler/\
spark_rapids_profile_converter \
  attempts/cupti-128-001/raw/profile/rapids-profile-*-driver.bin \
  > attempts/cupti-128-001/analysis/profile.json
```

Nsight Systems is the primary local profiler. CUPTI is an independent cross-check and a
possible production instrumentation path.

### JFR and device sampling

`scripts/run_jfr.sh` captured the whole local-mode JVM plus 100-ms `nvidia-smi`
sampling. JFR exposed only three Java file-read events and therefore provided no useful per-cell
read attribution; native/direct Hadoop and CUDA I/O may not appear as JFR FileRead.
The repeated local context and DEBUG filesystem timers are the relevant cache evidence.

### Nsight Compute

The preregistered Nsight Compute schedules and runner are retained, but NCU replay was
not executed. Nsight Systems answered the stage-overlap question with lower analysis
risk, and the user explicitly preferred it for local work. NCU remains the next tool
only if a kernel-level occupancy question cannot be answered from the coarse metrics.

## Results

See `NSYS_RESULTS.md`, `analysis/nsys-summary.json`, and `MODEL_VALIDATION.md`.

- 128 MiB: 87 tasks, 569-ms Spark stage, 557.1-ms GPU-ownership envelope,
  124.9-ms kernel service, 10,962 kernels,
  444.0-ms Parquet-decode NVTX union.
- 2,048 MiB: five tasks, 279-ms Spark stage, 266.3-ms ownership envelope,
  34.3-ms kernel service, 735 kernels,
  109.7-ms decode union.
- 16,384 MiB: one task, 469-ms Spark stage, 456.3-ms ownership envelope,
  30.1-ms kernel service, 324 kernels,
  90.2-ms decode union.

Aggregate CUDA memcpy trace bytes are stable at approximately 1.68 GiB. Therefore the high-end slowdown is
not more data or more kernel work: it is loss of task/kernel overlap and idle gaps. The
small-end slowdown is consistent with repeated task/decode setup and undersized work.

Profiled wall times are diagnostics and are not merged with the parent experiment's
unprofiled timing distributions.

## Reproducibility and audit rules

`provenance/manifest.txt` covers preregistration, scripts, summarized results, stdout, event
logs, journals, JFR, CUPTI binaries, and Nsight reports. Absolute paths in stdout and
tool metadata are provenance, not portable inputs. Generated SQLite and expanded CUPTI
JSON are intentionally excluded and reproducible using the commands above.

The raw taxi dataset remains external to Git and is identified by the parent
experiment's manifest. No network dataset was downloaded during profiling.

## Known limits

- only one hardware/storage/cache context;
- three profiled partition cells and no profiler repetitions;
- profiler perturbation prevents using these walls as confirmatory performance timing;
- stage ID 4 depends on the frozen schedule;
- no direct production metric yet exposes CUDA kernel busy-union time;
- the local history store is driver-owned and single-instance in this POC;
- predictive-error acceptance targets remain pending prospective holdout runs.
