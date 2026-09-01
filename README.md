# Databricks Data Profiling Demo — Caught-in-Air Model

A self-contained demo of **Databricks Lakehouse Monitoring** on a model inference table.
It generates a synthetic "caught in air" baseball inference table, with a deliberate
drift-and-degradation story baked in, then walks through creating an **Inference** monitor
on it. Because the table carries predictions, a ground-truth label, and a model id, the
monitor covers **data profiling, drift, and model quality** (accuracy) over time. No model
is trained; the catch probabilities are synthesized directly.

## What's here

| File | Purpose |
|---|---|
| `00_config` | Set the target **catalog** and **schema** (widgets). Every other notebook `%run`s this. |
| `01_generate_caught_in_air` | Build `caught_in_air_inference` (~120k rows over 35 days). |
| `02_data_profiling_demo` | Confirm the drift in-data, UI steps for the monitor, and queries against the metric tables. |
| `sql/prediction_quality_alert.sql` | Standalone query for a Databricks SQL Alert that fires when model accuracy drifts down (with a prediction-drift alternative). |

## Quickstart

1. Import all three notebooks into a Databricks workspace (same folder, so `%run ./00_config` resolves).
2. Open **`00_config`**, set the `catalog` and `schema` widgets (defaults: `main` / `dqm`), and run it.
3. Run **`01_generate_caught_in_air`** to build the table.
4. Open **`02_data_profiling_demo`** and follow it: confirm the drift, then create the monitor in the UI.

The catalog/schema are set once in `00_config`; the other notebooks inherit them and the
SQL cells use unqualified table names.

## The drift story

Around **day 22** a sensor recalibration shifts measured `launch_speed` up ~4 mph, and the
model rolls from `caught_v1` to `caught_v2`, which is not calibrated for the shifted inputs.
On **day 28** a bad batch lands with null `hang_time` and out-of-range `launch_angle`.

| Signal | What moves |
|---|---|
| Feature drift | `launch_speed` mean ~88 → ~92 mph |
| Prediction drift | `predicted_caught` rate ~0.62 → ~0.83 (v2 over-predicts) |
| Model quality | accuracy ~0.94 (`caught_v1`) → ~0.76 (`caught_v2`); `actual_caught` stays ~0.61 |
| Data quality | `hang_time` null spike, `launch_angle` goes negative |

Tune `N`, `DAYS`, `CHANGE_DAY`, `BAD_DAY` at the top of `01_generate_caught_in_air` to
reshape it.

## Table schema

| Group | Columns |
|---|---|
| Keys / monitoring | `play_id`, `event_timestamp`, `model_version` (`caught_v1` / `caught_v2`) |
| Features | `launch_speed`, `launch_angle`, `hang_time`, `spray_angle`, `hit_distance`, `fielder_position`, `fielder_start_distance`, `stadium` |
| Model output | `caught_probability` (0–1), `predicted_caught` (0/1) |
| Ground truth | `actual_caught` (0/1) |

## Create the monitor (Inference)

The Inference profile is built for model output tables: per-window data profiling, drift
(each window vs. the prior one), and model-quality metrics (accuracy and friends) when a
label is provided, all sliced by model id.

1. In Catalog Explorer, open your `caught_in_air_inference` table.
2. **Quality** tab → **Create monitor**.
3. **Profile type:** Inference.
4. **Problem type:** Classification.
5. **Prediction column:** `predicted_caught`.
6. **Label column:** `actual_caught` (unlocks the model-quality metrics).
7. **Model ID column:** `model_version` (metrics computed per model id, so v1 vs v2 separate).
8. **Timestamp column:** `event_timestamp`.
9. **Granularities:** `1 day`.
10. **Slicing expressions (optional):** `fielder_position`, `stadium`.
11. Leave output defaults (metric tables land in the same schema); pick a SQL warehouse for
    the dashboard.
12. **Create**, then **Refresh**. The first refresh backfills all 35 days.

Inference drift is consecutive-window by default (each day vs. the prior day), so no
baseline table is needed. To compare against a fixed reference (for example a healthy
`caught_v1` slice), set a baseline table in the monitor config instead.

## Alert on prediction-quality drift

`sql/prediction_quality_alert.sql` returns the most recent window's model accuracy from the
monitor's profile metrics table. Attach a Databricks SQL Alert to it:

1. SQL editor → paste the query → set the `catalog` / `schema` named parameters (defaults
   `main` / `dqm`) → run to confirm it returns a row. The table name is built from those
   parameters with `IDENTIFIER()`, so they fill a real object reference, not a value slot.
2. Save the query, then **Create alert** on it: column `latest_accuracy`, condition **is
   below**, threshold `0.85`.
3. Schedule it and add a destination (email / Slack / webhook).

In this demo the alert fires once `caught_v2` takes over and accuracy falls from ~0.94 to
~0.76. The file also includes a commented alternative that alerts on `predicted_caught`
drift (Jensen-Shannon distance) instead of accuracy. Requires the monitor to have refreshed
at least once.

## Tear down / rebuild

- **Rebuild:** re-run `01_generate_caught_in_air` (overwrites the table).
- **Tear down:** delete the monitor from the Quality tab (removes its metric tables), then
  `DROP TABLE IF EXISTS <catalog>.<schema>.caught_in_air_inference;`.

If you rebuild after creating a monitor, delete and recreate the monitor (or refresh it) so
its history matches the fresh data.

## Requirements

A Databricks workspace with Unity Catalog and Lakehouse Monitoring enabled, and a catalog
you can create a schema in. The generator runs on serverless or classic compute.
