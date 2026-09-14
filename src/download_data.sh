#!/usr/bin/env bash
# Download NYC TLC trip-record parquet files into data/raw/.
#
#   ./src/download_data.sh          # small SAMPLE (fast): yellow 2019-01 + zone lookup
#   ./src/download_data.sh --full   # FULL project set: yellow + fhvhv, 2019/2020/2025 (~16 GB)
#
# Files are gitignored — they live on disk, not in the repo.
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="https://d37ci6vzurychx.cloudfront.net"
OUT="data/raw"
mkdir -p "$OUT"

dl() {   # $1 = filename under /trip-data/
  echo "  -> $1"
  curl -fL --retry 3 -o "$OUT/$1" "$BASE/trip-data/$1" \
    || echo "     (skipped — not available: $1)"
}

# small dimension table for the join lessons (borough / zone names)
echo "zone lookup:"
curl -fL -o "$OUT/taxi_zone_lookup.csv" "$BASE/misc/taxi_zone_lookup.csv"

if [[ "${1:-}" == "--full" ]]; then
  echo "FULL set — yellow + fhvhv, 2019 / 2020 / 2025 (~16 GB, this takes a while):"
  for y in 2019 2020 2025; do
    for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
      dl "yellow_tripdata_${y}-${m}.parquet"
      dl "fhvhv_tripdata_${y}-${m}.parquet"   # fhvhv 2019 starts in Feb; Jan is skipped automatically
    done
  done
else
  echo "SAMPLE mode (pass --full for the whole ~16 GB set):"
  dl "yellow_tripdata_2019-01.parquet"
fi

echo "done. total size:"; du -sh "$OUT"
