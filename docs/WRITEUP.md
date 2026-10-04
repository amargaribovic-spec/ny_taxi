# Technical write-up — NYC taxi vs rideshare (PySpark)

Phase 3 of the FEP Data program. A PySpark pipeline over NYC TLC trip records, built to
answer one question and built small on purpose.

## 1. The question

**Did NYC rides recover after COVID, or did their shape change?** Specifically: how did
**yellow taxi** and **app-based rideshare** (High-Volume FHV — Uber/Lyft/Via) each move
across three checkpoints — **2019 (baseline) · 2020 (collapse) · 2025 (now)**?

Scope was deliberately narrow (the lesson from a previous project that sprawled to 18
marts): **one north-star question, 2 silver tables, 5 gold marts** covering volume, time,
geography, and mode shift.

## 2. Architecture

A **medallion** pipeline, run entirely inside one Docker container (Spark + JDK + Python +
JupyterLab from `quay.io/jupyter/pyspark-notebook`, digest-pinned):

```
data/raw (bronze)  ->  data/staging (silver)  ->  data/marts (gold)
 downloaded parquet     cleaned & conformed        business aggregations
```

- **No intermediate layer** — that's a dbt convention, not a Spark one. Three tiers.
- **Code mirrors data**: `src/staging/` builds `data/staging`, `src/marts/` builds `data/marts`.
  Shared logic lives in `src/common.py` (not a layer): `get_spark`, the layer paths,
  `in_study_window`, and `trips_with_mode` (the cross-mode loader every comparison mart reuses).
- Jobs read the previous layer and write Parquet to the next — the Spark equivalent of a
  dbt `ref()` chain. Marts read silver, never raw.
- `./run.sh pipeline` runs the whole chain in order, timed.

## 3. Findings

Measured over the aligned study window (see decision 1 below). Because 2019 covers 11
months (Feb–Dec) and 2020/2025 cover 12, recovery is stated as **per-month averages** so
the different month counts don't bias it.

**Volume — recovery is uneven.**

| mode | 2019 /mo | 2025 /mo | recovered to |
|------|---------:|---------:|-------------:|
| yellow | ~6.99M | ~4.06M | **~58%** |
| rideshare | ~21.3M | ~20.3M | **~95%** |

Rideshare is essentially back; yellow is stuck at ~58% of its pre-COVID baseline.

**Mode shift — COVID locked in a permanent gain for rideshare.** Rideshare's share of all
trips: **75.3% (2019) → 85.3% (2020) → 83.3% (2025)**. The pandemic jumped it +10 points as
yellow collapsed harder, and 2025 settled ~8 points above 2019 — the shift didn't reverse.

**Geography — yellow is a Manhattan service; rideshare is citywide.** ~89% of yellow
pickups originate in Manhattan. Outside it, rideshare is 91% of trips in Queens and
**98–99.9%** in Brooklyn, the Bronx, and Staten Island.

**Time of day — same peak, different purpose.** Both modes peak at **18:00** and bottom at
4–5am. But rideshare's dominance over yellow is highest overnight (~8× at 4am) and lowest
midday (~3.4× at noon) — yellow holds its best *relative* share in business hours, rideshare
owns the night.

**Headline:** the recovery is a *shift to rideshare*, not a return to how things were.

## 4. Key decisions (and why)

1. **Aligned the study window to Feb 2019.** HVFHV data begins Feb 2019; yellow has Jan 2019.
   Leaving that in would inflate yellow's 2019 totals and skew mode share. One shared filter
   (`in_study_window`) drops yellow's Jan 2019 so both modes span identical months. Cost: one
   month of baseline, for strictly comparable modes.
2. **Handled real schema drift.** `passenger_count` is `double` in 2019/2020 but `bigint` in
   2025 — a mixed-year read fails. Fix in silver: read each consistent-schema group separately,
   cast every column to an explicit type, then `unionByName`.
3. **Conformed rideshare to the yellow silver schema** (renamed `trip_miles -> trip_distance`,
   `base_passenger_fare -> fare_amount`, …) so the two modes stack with `unionByName` — the
   basis of every cross-mode mart.
4. **Filtered dirty timestamps** to the study years (stray pickups dated 2001, 2058, etc.).
5. **Kept it DRY** — one `trips_with_mode` loader, one `in_study_window` rule, reused across
   marts rather than copy-pasted.
6. **Performance** — column pruning, predicate pushdown, a broadcast join, map-side partial
   aggregation, AQE, and the memory tuning (`SPARK_CORES`, `maxPartitionBytes`,
   `--driver-memory`). Full detail, with measurements, in **[PERFORMANCE.md](PERFORMANCE.md)**.

## 5. Scaling & trade-offs

Answers to the questions this design invites:

- **What breaks at 10× / 100×?** The binding constraint here is a single JVM's heap. At 10×
  this box stops working: the silver rebuild already runs memory-throttled to 4 cores. The fix
  isn't a bigger box — it's **more boxes** (a cluster), where the work distributes across
  executors.
- **What would change on a real cluster?** Most of the local-mode tuning disappears:
  `--driver-memory` and the `SPARK_CORES` cap are artifacts of driver-is-executor local mode;
  on a cluster you size executors instead. Storage moves from local disk to object storage
  (S3/GCS). Critically, **`repartition` — which failed catastrophically here** (a full shuffle
  that OOM'd a single JVM) — becomes cheap and often *necessary* on a cluster, where the shuffle
  is spread across machines. The right tool depends on the hardware.
- **Which optimization mattered most?** Not a knob — the **structural** choices: column pruning
  (read 1 of 7 columns) and map-side partial aggregation (780M rows → ~960 shuffled). The
  broadcast join made the zone mart the fastest despite a join. Config tuning barely moved the
  needle; design did.
- **What would I do differently?** On a cluster: partition silver by year for read pruning; and
  combine the four cross-mode marts into one job that caches the 780M-row silver once instead of
  re-scanning it four times — infeasible on this box (won't fit 5 GB), natural with more memory.
  I'd also consider a table format (Delta/Iceberg) for schema evolution and time-travel.

## 6. What I learned

- **Measure before optimizing.** The repartition regression — textbook fix, catastrophic result
  — only showed up because I measured. "Try it and revert with a reason" is the real skill.
- **Optimization is hardware-dependent.** The same change (a shuffle) is a disaster on one JVM
  and a best practice on a cluster.
- **Reading Spark** — `explain()` plans and the live UI at `localhost:4040` — is half the job.
  Seeing 780M input rows become a 960-row shuffle made partial aggregation concrete.
- **Scope discipline.** Two silver + five gold answered the question fully; sprawl would have
  added noise, not insight.

## 7. Pointers

- **[README](../README.md)** — setup and how to run.
- **[PERFORMANCE.md](PERFORMANCE.md)** — the optimization detail, baselines, and measurements.
