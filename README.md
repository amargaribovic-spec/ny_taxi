# ny_taxi — NYC Trip Records with PySpark

Phase 3 of the FEP Data program. A **PySpark** project over the NYC TLC trip-record
data, run entirely inside a **Docker** container — Spark, Java and JupyterLab all
bundled, so there's nothing to install on the host but Docker.

**Project question:** how NYC rides collapsed in 2020 and what recovered by 2025,
measured against the 2019 baseline — and how the balance shifted from yellow taxis
to app rideshare (HVFHV).

## What's in the container

`quay.io/jupyter/pyspark-notebook` gives a matched **Spark + JDK + Python + JupyterLab**;
the `Dockerfile` adds a couple of Python libs (`requirements.txt`). Nothing runs on
the host — you edit notebooks/scripts locally and they're mounted into the container.

## Quick start

```bash
# 1. build the image and start JupyterLab (first build pulls a large base image)
./run.sh up

# 2. get the browser URL (with token) and open it
./run.sh token

# 3. grab a small sample of data to test with (fast)
./run.sh download          # yellow 2019-01 + zone lookup, into data/raw/

# 4. run the Day-1 "first look" (schema, count, sample)
./run.sh submit src/first_look.py
```

When you're ready for the real dataset:

```bash
./run.sh download --full   # yellow + fhvhv, 2019 / 2020 / 2025 (~16 GB)
```

Other commands: `./run.sh shell` (bash inside the container), `./run.sh logs`,
`./run.sh down`. The **Spark UI** is at <http://localhost:4040> while a job runs —
that's where you read execution plans, stages and shuffles for the optimization
write-up.

## Structure

```
ny_taxi/
├── README.md
├── Dockerfile              # pyspark-notebook base + extra libs
├── docker-compose.yml      # the pyspark service (JupyterLab + Spark UI)
├── requirements.txt        # extra Python deps (polars, plotly)
├── run.sh                  # up / token / shell / submit / download / down
├── data/
│   └── raw/                # parquet files (gitignored) — downloaded, not committed
├── notebooks/              # exploratory JupyterLab notebooks
└── src/
    ├── download_data.sh    # fetch NYC TLC parquet into data/raw/
    └── first_look.py       # Day-1 read + profile
```

## Data

- **Source:** NYC Taxi & Limousine Commission trip records (public, Parquet).
- **Types:** yellow taxi + High-Volume FHV (Uber / Lyft / Via / Juno).
- **Years:** 2019 (baseline) · 2020 (collapse) · 2025 (recovery).
- The CSVs/parquet are **gitignored** — code lives in the repo, data lives on disk
  (`data/raw/`). Start with the sample, scale to `--full` when the pipeline works.

> Note: HVFHV data begins **Feb 2019**, so `fhvhv_tripdata_2019-01` doesn't exist —
> the download script skips it automatically.
