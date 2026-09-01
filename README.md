# Databricks Data Profiling Demo — Caught-in-Air Model

A self-contained demo of **Databricks Lakehouse Monitoring** used for **data profiling
and drift over time**. It generates a synthetic "caught in air" baseball inference table,
with a deliberate drift story baked in, then walks through creating a **Time series**
monitor on it. No model is trained — the catch probabilities are synthesized directly so
the focus stays on profiling and drift.

## What's here

| Notebook | Purpose |
|---|---|
| `00_config` | Set the target **catalog** and **schema** (widgets). Every other notebook `%run`s this. |
| `01_generate_caught_in_air` | Build `caught_in_air_inference` (~120k rows over 35 days). |
| `02_data_profiling_demo` | Confirm the drift in-data, UI steps for the monitor, and queries against the metric tables. |

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
| Prediction drift | `predicted_caught` rate ~0.76 → ~0.91 (v2 over-predicts) |
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

## Create the monitor (Time series)

Time series is the profile type for drift over time: profile stats per time window, plus
drift metrics comparing each window to the one before it.

1. In Catalog Explorer, open your `caught_in_air_inference` table.
2. **Quality** tab → **Create monitor**.
3. **Profile type:** Time series.
4. **Timestamp column:** `event_timestamp`.
5. **Granularities:** `1 day`.
6. **Slicing expressions (optional):** `model_version`, `fielder_position`, `stadium`.
7. Leave output defaults (metric tables land in the same schema); pick a SQL warehouse for
   the dashboard.
8. **Create**, then **Refresh**. The first refresh backfills all 35 days.

Time series drift is consecutive-window by default (each day vs. the prior day), so no
baseline table is needed. To compare against a fixed reference, set a baseline table in the
monitor config instead.

## Tear down / rebuild

- **Rebuild:** re-run `01_generate_caught_in_air` (overwrites the table).
- **Tear down:** delete the monitor from the Quality tab (removes its metric tables), then
  `DROP TABLE IF EXISTS <catalog>.<schema>.caught_in_air_inference;`.

If you rebuild after creating a monitor, delete and recreate the monitor (or refresh it) so
its history matches the fresh data.

## Requirements

A Databricks workspace with Unity Catalog and Lakehouse Monitoring enabled, and a catalog
you can create a schema in. The generator runs on serverless or classic compute.
