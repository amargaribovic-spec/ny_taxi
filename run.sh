#!/usr/bin/env bash
# run.sh — drive the ny_taxi PySpark container.
#
#   ./run.sh up                 build the image and start JupyterLab (background)
#   ./run.sh token              print the JupyterLab URL + token to open in the browser
#   ./run.sh logs               follow the container logs
#   ./run.sh shell              open a bash shell inside the container
#   ./run.sh submit <file> [..] spark-submit a script inside the container
#   ./run.sh download [--full]  download the NYC taxi parquet into data/raw/
#   ./run.sh down               stop the container
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE="docker compose"

case "${1:-help}" in
  up)
    $COMPOSE up -d
    printf "waiting for JupyterLab to start"
    url=""
    for _ in $(seq 1 30); do
      url="$($COMPOSE logs pyspark 2>&1 | grep -Eo 'http://127\.0\.0\.1:8888/lab\?token=[a-z0-9]+' | tail -1)"
      [ -n "$url" ] && break
      printf "."; sleep 1
    done
    echo
    if [ -n "$url" ]; then
      echo "JupyterLab: $url"
    else
      echo "not ready yet — give it a moment, then run './run.sh token'"
    fi
    echo "Spark UI (while a job runs): http://localhost:4040"
    ;;
  token)
    $COMPOSE logs pyspark 2>&1 | grep -Eo 'http://127\.0\.0\.1:8888/lab\?token=[a-z0-9]+' | tail -1 \
      || echo "no token yet — is it up? try './run.sh logs'"
    ;;
  logs)   $COMPOSE logs -f pyspark ;;
  shell)  $COMPOSE exec -w /home/jovyan/work pyspark bash ;;
  submit)
    shift || true
    # PYTHONPATH=src so jobs can `from common import ...`.
    # --driver-memory: in local mode the driver IS the executor, and the default
    # ~1g heap OOMs on the full data. 5g fits our 7.7g Docker; override with
    # SPARK_DRIVER_MEM=NNg ./run.sh submit ... on a smaller/bigger machine.
    # --master local[N]: N = concurrent tasks, each holding its own read/write
    # buffers in that one heap. Fewer cores = lower peak memory. Default all cores;
    # for heavy jobs (HVFHV) run e.g. SPARK_CORES=4 ./run.sh submit ...
    $COMPOSE exec -w /home/jovyan/work -e PYTHONPATH=/home/jovyan/work/src pyspark \
      spark-submit --master "local[${SPARK_CORES:-*}]" \
      --driver-memory "${SPARK_DRIVER_MEM:-5g}" "$@"
    ;;
  download)
    shift || true
    bash src/download_data.sh "$@"
    ;;
  rebuild) $COMPOSE up -d --build ;;   # only when the Dockerfile / requirements change
  down)    $COMPOSE down ;;
  *)
    echo "usage: ./run.sh [up|rebuild|token|logs|shell|submit <file>|download [--full]|down]"
    ;;
esac
