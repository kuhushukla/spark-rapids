# Shared core for every scan-split runner. Sourced by run_scan_bench.sh, run_window_bench.sh and
# run_learning_bench.sh so the conf block, the resume rule and the metric check cannot drift apart.
#
# The split heuristic has ONE formula and no knobs: split = batchSizeBytes / decodeRatio, clamped to
# [64 MiB, min(4 GiB, listedBytes / minPartitionNum)]. The old -Drapids.autotuner.* properties are
# gone. minPartitionNum comes from spark.sql.files.minPartitionNum, else defaultParallelism, so the
# old ceiling=core<N> (targetTasks = N x minPartitionNum) is exactly minPartitionNum = N*CORES;
# core1 is what the heuristic already does with the conf unset.

BENCH_CORES="${BENCH_CORES:-16}"          # --master local[N]
BENCH_SHIM="${BENCH_SHIM:-spark353}"      # shuffle-manager shim; must match the jar's buildver
BENCH_DRIVER_MEM="${BENCH_DRIVER_MEM:-32G}"
BENCH_PINNED="${BENCH_PINNED:-8g}"
BENCH_CONC_GPU="${BENCH_CONC_GPU:-2}"
BENCH_FREE_PATH="${BENCH_FREE_PATH:-/data}"   # filesystem the free-space guard watches

# history_conf <snapshot-path>
history_conf () { echo "--conf spark.rapids.sql.historyPath=$1"; }

# ceiling_conf <core|coreN> [cores] -- the only surviving ceiling lever. Non-core modes are rejected
# rather than silently ignored, since they had no equivalent once the ceiling stopped being tunable.
ceiling_conf () {
  local mode="$1" cores="${2:-$BENCH_CORES}" n
  case "$mode" in
    core*) n="${mode#core}"
           [ -n "$n" ] && [ -z "${n//[0-9]/}" ] || { echo "!! bad ceiling=$mode" >&2; return 2; }
           echo "--conf spark.sql.files.minPartitionNum=$((n*cores))" ;;
    *)     echo "!! ceiling=$mode not expressible against this heuristic (only core<N>)" >&2; return 2 ;;
  esac
}

# The split each scan planned with comes from the GpuScan 'scan max split bytes' DEBUG metric
# (prefix match, so logs under the older '... (effective)' name still pass). Every report reads the
# split from it, so a run without it is unreportable -- fail at the first arm, not after the last.
assert_split_metric () {
  local outd="$1" tag="$2"
  grep -aqs "scan max split bytes" "$outd"/el/* && return 0
  echo "  !! $tag: no 'scan max split bytes' metric in $outd/el"
  echo "  !! Jar predates the metric, the arm did not run with spark.rapids.sql.metrics.level=DEBUG,"
  echo "  !! or the scan fell back to the CPU. Aborting rather than reporting a blank split."
  return 4
}

# iters_done <run.log> <query> -- iterations already recorded, 0 when absent.
iters_done () {
  local n; n=$(grep -acE "ITER $2 [0-9]+ [0-9]+" "$1" 2>/dev/null; true)
  echo "${n//[^0-9]/}" | head -1
}

# spark_arm <outdir> <query> <iters> <maxPartitionBytes> [extra-confs] [extra-java-opts]
#
# Runs ONE arm and leaves run.log + el/ under <outdir>. Returns non-zero if the arm produced no
# iterations or no split metric, so a caller can skip or abort as it prefers.
#
# Required in the environment: SPARK_HOME JAR BENCH DATA LOCALDIR
# Optional:
#   GPU_UUID          pin to one GPU (never the T400)
#   AQE=on|off        spark.sql.adaptive.enabled           (default on)
#   NSYS=no|yes|cpu   wrap in nsys; cpu adds CPU sampling  (default no)
#   WHERE=<predicate> data window, applied at view registration by the bench scala
#   XCONF=<confs>     extra --conf(s) appended to EVERY arm
#   LINK_EVENTLOG=yes symlink el/<log> to eventlog-test-1, the name ab uses and build_ledger reads
#   MIN_FREE_GB=<n>   abort when /data falls below this (default 20; 0 disables)
spark_arm () {
  local outd="$1" q="$2" iters="$3" mpb="$4" xc="${5:-}" xj="${6:-}"
  local free_gb="${MIN_FREE_GB:-20}"
  if [ "$free_gb" -gt 0 ] 2>/dev/null; then
    local avail; avail=$(df -BG --output=avail "$BENCH_FREE_PATH" | tail -1 | tr -dc '0-9')
    [ "${avail:-0}" -lt "$free_gb" ] &&
      { echo "!! $BENCH_FREE_PATH < ${free_gb}G free - abort"; return 3; }
  fi
  mkdir -p "$outd/el" "$LOCALDIR"

  # -s none = no CPU sampling (small traces). NSYS=cpu adds CPU sampling + OS runtime, needed when
  # the GPU is idle most of the wall time and the bottleneck is host-side. Traces are much larger.
  local npre=""
  case "${NSYS:-no}" in
    yes) npre="/usr/local/bin/nsys profile -t cuda,nvtx -s none -o $outd/prof --force-overwrite true" ;;
    cpu) npre="/usr/local/bin/nsys profile -t cuda,nvtx,osrt -s cpu --cpuctxsw=process-tree -o $outd/prof --force-overwrite true" ;;
  esac
  local aqe=true; [ "${AQE:-on}" = off ] && aqe=false
  local wherec=(); [ -n "${WHERE:-}" ] && wherec=(--conf "spark.bench.where=$WHERE")

  echo "  run $q mpb=$mpb iters=$iters${WHERE:+ where=[$WHERE]}${NSYS:+ nsys=$NSYS} -> $outd  $(date +%T)"
  # filecache stays OFF: it serves a repeat read of the same table from cache, which transfers across
  # queries exactly like the split history does and would confound an inheritance measurement.
  ${GPU_UUID:+env CUDA_VISIBLE_DEVICES=$GPU_UUID} $npre \
  "$SPARK_HOME/bin/spark-shell" --master "local[$BENCH_CORES]" --driver-memory "$BENCH_DRIVER_MEM" \
    --conf spark.driver.maxResultSize=2GB --conf spark.local.dir="$LOCALDIR" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.variableFloatAgg.enabled=false \
    --conf spark.sql.files.maxPartitionBytes="$mpb" \
    --conf spark.sql.adaptive.enabled=$aqe \
    --conf spark.rapids.sql.metrics.level=DEBUG --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.filecache.enabled=false \
    --conf spark.rapids.memory.pinnedPool.size="$BENCH_PINNED" \
    --conf spark.rapids.sql.concurrentGpuTasks="$BENCH_CONC_GPU" \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.$BENCH_SHIM.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$outd/el" \
    "${wherec[@]}" ${XCONF:-} $xc \
    --driver-java-options "-Dbench.query=$q -Dbench.iters=$iters -Dbench.base=$DATA $xj" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$BENCH" < /dev/null > "$outd/run.log" 2>&1

  if [ "${LINK_EVENTLOG:-}" = yes ]; then
    local p; p="$(ls -t "$outd/el" 2>/dev/null | head -1)"
    [ -n "$p" ] && ln -sf "el/$p" "$outd/eventlog-test-1"
  fi
  local its; its="$(grep -aoE "ITER $q [0-9]+ [0-9]+" "$outd/run.log" | awk '{print $4}' | tr '\n' ' ')"
  [ -z "$its" ] && { echo "    !! $q: no iterations - see $outd/run.log"; return 1; }
  echo "    $q: $its"
  assert_split_metric "$outd" "$q" || return 4
}
