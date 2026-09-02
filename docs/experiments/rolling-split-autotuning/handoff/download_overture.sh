#!/usr/bin/env bash
# Overture Maps acquirer: syncs the released GeoParquet directly from the public S3 bucket (already parquet,
# so NO conversion step). Lays tables out exactly as the overture benches read them (bench.base = --out):
#   $OUT/places/type=place/            <- theme=places
#   $OUT/addresses/type=address/       <- theme=addresses
#   $OUT/divisions/type=division/      <- theme=divisions
#   $OUT/transportation/type=segment/  <- theme=transportation
#   $OUT/transportation/type=connector/
# Needs: awscli. Public bucket -> --no-sign-request (no credentials).
#
# Usage: download_overture.sh [--out=DIR] [--release=TAG] [--only=TABLE[,TABLE...]] [--max-files=N] [--list-only]
#   default out=/data/overture, release=2026-07-22.0. Tables: place,address,division,segment,connector.
#   --max-files=N syncs only the first N parquet files per table (smoke-sized sample).
#   list releases: aws s3 ls --no-sign-request s3://overturemaps-us-west-2/release/
set -uo pipefail
OUT=/data/overture
REL=2026-07-22.0
ONLY=all
LIST_ONLY=no
MAXFILES=0
for a in "$@"; do case "$a" in
  --out=*)       OUT="${a#*=}" ;;
  --release=*)   REL="${a#*=}" ;;
  --only=*)      ONLY="${a#*=}" ;;
  --max-files=*) MAXFILES="${a#*=}" ;;
  --list-only)   LIST_ONLY=yes ;;
  -h|--help)     sed -n '2,15p' "$0"; exit 0 ;;
  *) echo "unknown arg: $a  (see --help)"; exit 2 ;;
esac; done
command -v aws >/dev/null || { echo "!! awscli not found (needed for s3 sync)"; exit 3; }
S3="s3://overturemaps-us-west-2/release/$REL"
echo ">> config: out=$OUT release=$REL only=$ONLY max-files=$MAXFILES  src=$S3"

# table -> "theme/type=<t>" (dst path == read path used by the benches)
declare -A PATHS=(
  [place]=places/type=place
  [address]=addresses/type=address
  [division]=divisions/type=division
  [segment]=transportation/type=segment
  [connector]=transportation/type=connector
)
if [ "$ONLY" = all ]; then tables=(place address division segment connector); else IFS=',' read -ra tables <<< "$ONLY"; fi

for t in "${tables[@]}"; do
  rel="${PATHS[$t]:-}"
  [ -z "$rel" ] && { echo "   [skip] unknown table '$t' (place|address|division|segment|connector)"; continue; }
  theme="${rel%%/*}"; typ="${rel#*/}"
  src="$S3/theme=$theme/$typ/"
  dst="$OUT/$rel/"
  if [ "$LIST_ONLY" = yes ]; then
    echo ">> $t: aws s3 ls $src"; aws s3 ls --no-sign-request "$src" | head -3; continue
  fi
  mkdir -p "$dst"
  echo ">> $t -> $dst"
  if [ "$MAXFILES" -gt 0 ] 2>/dev/null; then
    # Sample mode: copy the first N objects rather than syncing the whole table.
    aws s3 ls --no-sign-request "$src" | awk '{print $4}' | grep -E '\.parquet$' | head -"$MAXFILES" |
      while read -r f; do
        [ -s "$dst$f" ] && { echo "   [have] $f"; continue; }
        aws s3 cp --no-sign-request --only-show-errors "$src$f" "$dst$f" && echo "   [get]  $f"
      done
  else
    aws s3 sync --no-sign-request "$src" "$dst"
  fi
done
[ "$LIST_ONLY" = yes ] && { echo "(list-only) done."; exit 0; }
echo ">> on disk:"; du -sh "$OUT" 2>&1
echo ">> done -> $OUT   (set bench.base=$OUT)"
