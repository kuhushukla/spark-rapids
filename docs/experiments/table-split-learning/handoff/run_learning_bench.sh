#!/usr/bin/env bash
# Table-split-learning benchmark. Dataset-agnostic: NDS on the sparkh cluster via the ab repo, or the
# local clickstream/pageviews/overture kit via spark-shell. Same arms, same ledger, same report.
#
# ONE APPLICATION PER QUERY in every arm. This matters: the first query in an application pays a
# large scan-task semaphore-wait penalty (~43 s, visible on query9's cold row in every arm of the
# 20260820b run), so an arm that runs a query second and an arm that runs it first are not
# comparable. Giving every query its own application equalises that position, leaving the history
# file contents as the only difference.
#
# It also makes the shared arms a genuine CROSS-APPLICATION test: app 2 reads a record written by a
# different JVM, so learning has to survive a restart rather than living in one session.
#
#   off      : one app per query, no autotuner conf at all, writes no history
#   shared   : app1 qA -> shared.tsv ; app2 qB -> SAME shared.tsv   (qB inherits qA's split)
#   isolated : app1 qA -> iso-qA.tsv ; app2 qB -> iso-qB.tsv        (each learns only from itself)
#
# CEILINGS defaults to "core1" only. `none` was dropped after the 20260820c run: its inherited split
# landed 33% away from where the query converges and every metric was worse, and core1 is what we
# would ship. Set CEILINGS="core1 none" to bring it back.
#
# Usage:
#   DATASET=nds        ./run_learning_bench.sh              # queries default: query9 query28
#   DATASET=clickstream QUERIES="csH cs03" ./run_learning_bench.sh
#
# ab granularity, verified in ab/platforms.py:1233-1293:
#   --queries q      -> --sub_queries      run only that query
#   --iterations N   -> N executions of it, consecutive
#   --runs R         -> R spark-submit calls (kept at 1; the per-query apps give us the restart)
set -euo pipefail
export JAVA_HOME="${BENCH_JAVA_HOME:-${JAVA_HOME:-/usr/lib/jvm/java-1.17.0-openjdk-amd64}}"

DATASET="${DATASET:-nds}"
TAG="${TAG:-$(date +%Y%m%d)}"
ITERS="${ITERS:-5}"
CEILINGS="${CEILINGS:-core1}"
OUTROOT="${OUTROOT:-/data/table-split-learning-${TAG}}"

AB=/home/kuhu/Reps/ab
KIT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../rolling-split-autotuning/handoff" && pwd)"
. "$KIT/bench_common.sh"

case "$DATASET" in
  nds)
    BACKEND=ab
    QUERIES="${QUERIES:-query9 query28}"
    JAR="${RAPIDS_JAR:-/home/kuhu/Reps/spark-rapids/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar}"
    WANT_SHIM=spark357            # sparkh runs Spark 3.5.7
    BENCH_SHIM=spark357
    # historyPath lives on the cluster driver's local fs, not in $OUTROOT (which is a local dir)
    HIST_DIR="/tmp/tsl-${TAG}"
    ;;
  clickstream|pageviews|overture)
    BACKEND=local
    BENCH_SHIM=spark353
    case "$DATASET" in
      clickstream) QUERIES="${QUERIES:-csH cs03}"; DATA="${DATA:-/data/wiki-clickstream/parquet}"
                   BENCH="$KIT/bench_clickstream.scala" ;;
      pageviews)   QUERIES="${QUERIES:-pvH pv03g}";  DATA="${DATA:-/data/wiki-pageviews/parquet}"
                   BENCH="$KIT/bench_pageviews.scala" ;;
      overture)    QUERIES="${QUERIES:-hs3 gf1}";    DATA="${DATA:-/data/overture/parquet}"
                   BENCH="$KIT/bench_overture.scala" ;;
    esac
    # The local box has a T400 driving the display on index 0. CUDA_DEVICE_ORDER=FASTEST_FIRST
    # reverses nvidia-smi's ordering, so the A5000 must be selected by UUID, never by index.
    JAR="${RAPIDS_JAR:-/home/kuhu/Reps/spark-rapids/dist/rapids-353-local-keep.jar}"
    WANT_SHIM=spark353
    # NOT ${SPARK_HOME:-...}: this box exports SPARK_HOME=spark-3.3.3, so a default would never
    # apply and spark-shell would launch 3.3.3 against a 353 jar. Override via TSL_SPARK_HOME.
    SPARK_HOME="${TSL_SPARK_HOME:-/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3}"
    export SPARK_HOME
    GPU_UUID="${GPU_UUID:-GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504}"
    LOCALDIR="${LOCALDIR:-/data/sparklocal-tsl-${TAG}}"
    MPB="${MPB:-2g}"              # same maxPartitionBytes as the NDS arms, so the cold fallback matches
    HIST_DIR="$OUTROOT/hist"
    ;;
  *) echo "!! unknown DATASET=$DATASET (nds|clickstream|pageviews|overture)"; exit 2 ;;
esac

export BENCH_SHIM     # set per-backend above; spark_arm builds the shuffle-manager name from it
[ -f "$JAR" ] || { echo "!! jar not found: $JAR"; exit 2; }

# The plugin refuses to initialise if the Spark it is loaded into does not match its shim, and that
# failure only shows up after a full spark-shell launch. Check it here instead.
if [ "$BACKEND" = local ]; then
  SPARK_VER="$(head -1 "$SPARK_HOME/RELEASE" 2>/dev/null | awk '{print $2}')"
  WANT_VER="${WANT_SHIM#spark}"; WANT_VER="${WANT_VER:0:1}.${WANT_VER:1:1}.${WANT_VER:2:1}"
  [ "$SPARK_VER" = "$WANT_VER" ] || {
    echo "!! SPARK_HOME=$SPARK_HOME is Spark ${SPARK_VER:-unknown}, but the jar is a $WANT_SHIM build."
    echo "!! Set TSL_SPARK_HOME to a Spark $WANT_VER install."; exit 2; }
  echo ">> spark=$SPARK_VER at $SPARK_HOME"
fi

mkdir -p "$OUTROOT" "$HIST_DIR"
MANIFEST="$OUTROOT/manifest.json"
echo ">> dataset=$DATASET backend=$BACKEND queries=[$QUERIES] ceilings=[$CEILINGS]"
echo ">> jar=$JAR  tag=$TAG iters=$ITERS out=$OUTROOT"

# The manifest is what makes the ledger dataset-agnostic: build_ledger.py reads the arm list, its run
# order and each arm's history file from here instead of carrying a hardcoded table of NDS arm names.
# Written incrementally so a run killed part-way still parses.
: > "$MANIFEST.tmp"
manifest_add() {  # arm ceiling history_mode query history_file
  printf '{"arm":"%s","ceiling":"%s","history_mode":"%s","query":"%s","history_file":"%s","dataset":"%s","backend":"%s"}\n' \
    "$1" "$2" "$3" "$4" "$5" "$DATASET" "$BACKEND" >> "$MANIFEST.tmp"
  python3 -c 'import json,sys
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
json.dump({"tag":sys.argv[2],"dataset":sys.argv[3],"iters":int(sys.argv[4]),"arms":rows},
          open(sys.argv[5],"w"), indent=1)' "$MANIFEST.tmp" "$TAG" "$DATASET" "$ITERS" "$MANIFEST"
}

run_ab() {   # arm template query
  local arm="$1" tpl="$2" q="$3" out="$OUTROOT/$1"
  [ -f "$AB/templates/onprem-h/$tpl" ] || { echo "!! missing template $tpl"; exit 2; }
  mkdir -p "$out"
  ( cd "$AB" && python3 ab.py --platform onprem-h \
      --test_template "templates/onprem-h/$tpl" \
      --test_jar "$JAR" \
      --queries "$q" \
      --iterations "$ITERS" \
      --runs 1 \
      --capture_eventlog \
      --output "$out" )
}

run_local() {   # arm query history_path ceiling
  local arm="$1" q="$2" hp="$3" ceiling="$4" out="$OUTROOT/$1"
  local xc="" xj="" cc
  if [ "$ceiling" != off ]; then
    cc="$(ceiling_conf "$ceiling")" || return 1
    xc="$(history_conf "$hp") $cc"
  fi
  LINK_EVENTLOG=yes spark_arm "$out" "$q" "$ITERS" "$MPB" "$xc" "$xj"
}

run_one() {   # arm ceiling history_mode query history_file_basename
  local arm="$1" ceiling="$2" mode="$3" q="$4" hf="$5"
  echo "########## $arm  query=$q  $(date +%H:%M:%S) ##########"
  if [ "$BACKEND" = ab ]; then
    local tpl
    if [ "$ceiling" = off ]; then tpl="tsl-${TAG}-off.template"
    elif [ "$mode" = shared ]; then tpl="tsl-${TAG}-${ceiling}-shared.template"
    else tpl="tsl-${TAG}-${ceiling}-iso-${q}.template"; fi
    run_ab "$arm" "$tpl" "$q"
  else
    run_local "$arm" "$q" "$HIST_DIR/${hf}.snapshot" "$ceiling"
  fi
  manifest_add "$arm" "$ceiling" "$mode" "$q" "$hf"
  echo "########## $arm done $(date +%H:%M:%S) ##########"
}

# baseline: no autotuner, one app per query
for q in $QUERIES; do run_one "off-$q" off none "$q" ""; done

# shared history: the FIRST query writes the record, later queries inherit it FROM ANOTHER JVM.
# Query order within this loop is the inheritance chain, so it is the order in $QUERIES.
for c in $CEILINGS; do
  for q in $QUERIES; do run_one "${c}-shared-$q" "$c" shared "$q" "${c}-shared"; done
done

# isolated history: each query against its own file, so no query can see another's record
for c in $CEILINGS; do
  for q in $QUERIES; do run_one "${c}-iso-$q" "$c" isolated "$q" "${c}-iso-$q"; done
done

echo "ALL ARMS DONE $(date +%H:%M:%S) -> $OUTROOT"
echo "next: python3 build_ledger.py $OUTROOT && python3 gen_learning_report.py"
