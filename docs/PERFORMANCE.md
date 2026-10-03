# Performance notes

How this pipeline performs, why it's built the way it is, and one optimization that
backfired. Everything here was measured on the actual box, not assumed.

## The box

Everything runs in **one Docker container** (`quay.io/jupyter/pyspark-notebook`), Spark
in **local mode** — the driver *is* the executor, one JVM, ~7.7 GB RAM, `--driver-memory 5g`.
That single fact drives every decision below: the pipeline is **memory-bound**, not
compute-bound. The binding constraint is heap, so the goal is to move as little data
through memory as possible and to avoid shuffles.

## Baseline

Full `raw -> silver -> gold` run (`./run.sh pipeline`):

| stage | seconds |
|-------|--------:|
| stg_yellow | 196 |
| stg_fhvhv | 619 |
| trips_by_year | 77 |
| trips_by_period | 105 |
| trips_by_hour | 145 |
| trips_by_zone | 70 |
| mode_shift_yoy | 145 |
| **TOTAL** | **1357** (~22.6 min) |

`stg_fhvhv` dominates (~46%): it reads ~10 GB of the largest TLC files and is deliberately
throttled to 4 cores for memory (see below). The marts each re-scan the ~780M-row silver,
which on a single JVM can't be cached away (780M rows won't fit a 5 GB heap), so each is an
independent `spark-submit`.

## What the pipeline gets right (and how we know)

Read with `./run.sh submit src/explore/explain_marts.py` (plans) and the Spark UI at
`localhost:4040` while a job runs (live metrics).

1. **Column pruning.** Marts read one column off disk. Plan: `ReadSchema: struct<pu_location_id:int>`
   (1 of 7 columns). Because silver is Parquet (columnar), the other columns are never read.

2. **Predicate pushdown.** The join's null-check is pushed into the reader:
   `PushedFilters: [IsNotNull(pu_location_id)]` — junk rows are dropped at scan time.

3. **Broadcast join** (`trips_by_zone`). The zone lookup (265 rows, 12 KB) is broadcast, so
   the 780M-row side never moves: `BroadcastHashJoin ... BuildRight` fed by a `BroadcastExchange`,
   and **no `Exchange` above the `Union`** of the big tables. Fastest mart (70s) despite a join.

4. **Map-side partial aggregation.** `groupBy().count()` pre-counts per partition before the
   shuffle: `HashAggregate(partial_count)` -> `Exchange` -> `HashAggregate(count)`. Measured on
   `trips_by_hour`: each task turned **millions of input rows into exactly 24 partial counts**
   (one per hour), so the whole stage shuffled **~960 rows / 73 KiB** instead of 245M+ rows.
   The group-by over 780M rows is cheap because almost nothing crosses the network.

5. **Adaptive Query Execution.** Plans show `AdaptiveSparkPlan isFinalPlan=false` and
   `Exchange hashpartitioning(..., 200)`; at runtime AQE coalesces those 200 partitions down to
   ~1–2 because the output is tiny (<= 72 groups). This is why hand-tuning
   `spark.sql.shuffle.partitions` isn't worth it here.

## Memory tuning (the journey)

The big HVFHV job forced three settings, all earned by hitting the wall:

- **`--driver-memory 5g`** (in `run.sh`). The default ~1 GB heap OOMs on the full data; the
  driver is the executor in local mode, so it needs the room. 5g fits the 7.7 GB container.
- **`SPARK_CORES` knob** (`--master local[N]`). In local mode, **N = concurrent tasks, each
  holding its own read/write buffers in the one heap.** `stg_fhvhv` OOMed on the write at 12
  cores (12 concurrent Parquet writers + Snappy buffers blew the heap). Capping to 4 cores cut
  peak memory to a third and it completed. Fewer cores = slower but survivable — the pipeline
  auto-caps `stg_fhvhv` to 4.
- **`spark.sql.files.maxPartitionBytes = 32m`** (in `stg_fhvhv`). Smaller read splits so each
  task decodes a smaller slice of the hard-compressing HVFHV files.

## One optimization that backfired (and the lesson)

**Hypothesis:** the `fhvhv` silver lands as **206 wildly uneven files (897 B – 379 MB)** — a
small-files/skew problem — so `repartition(48)` before the write should even them out and speed
every downstream mart.

**Result: catastrophic regression.** `repartition` is a **full shuffle**, and `stg_fhvhv` was
otherwise *narrow* (read -> filter -> write, no data movement). Adding a shuffle of 621M rows to
a memory-constrained single JVM caused it to GC-thrash for ~50 minutes until the executor missed
60 heartbeats and Spark shut the context down (`Exit as unable to send heartbeats to driver`,
then cascading `FetchFailed` / `CommitDenied`). Reverted.

**Lesson:** on a memory-bound single-JVM box, any optimization that **adds a shuffle**
(`repartition`, re-keying, a non-broadcast join) risks being far worse than the problem it
targets. The 206 ugly files were functional; column pruning makes even the big ones cheap to
scan (measured: `trips_by_hour` task durations were near-even, median 19s vs max 22s, despite
the file skew). The fix cost more than the disease.

## Conclusion

The pipeline already uses every lever that matters for this hardware — prune, pushdown,
broadcast, partial-aggregate, AQE — with no config tuning. On a single memory-bound container
the remaining "wins" (repartitioning, caching the 780M-row silver) are infeasible or harmful.
The real optimization work here was **design** (narrow jobs + broadcast + pruning) and
**judgment** (measure, and revert a change that regresses), not turning knobs.

## Reproduce

```bash
./run.sh pipeline                            # timed raw -> silver -> gold
./run.sh submit src/explore/explain_marts.py # physical plans
# then open http://localhost:4040 while any job runs for live stage/task metrics
```
