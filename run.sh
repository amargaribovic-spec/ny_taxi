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
    $COMPOSE up -d --build
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
    $COMPOSE exec -w /home/jovyan/work pyspark spark-submit "$@"
    ;;
  download)
    shift || true
    bash src/download_data.sh "$@"
    ;;
  down)   $COMPOSE down ;;
  *)
    echo "usage: ./run.sh [up|token|logs|shell|submit <file>|download [--full]|down]"
    ;;
esac
