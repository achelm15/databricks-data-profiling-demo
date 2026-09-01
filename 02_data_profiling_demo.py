# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Data Profiling Demo: Caught-in-Air Model
# MAGIC
# MAGIC Walks through a **Lakehouse Monitoring** data-profiling demo on the synthetic
# MAGIC `caught_in_air_inference` table. The focus is **profiling and drift over time**.
# MAGIC
# MAGIC **Prereqs:** run `01_generate_caught_in_air` first to build the table.
# MAGIC
# MAGIC **What you will do**
# MAGIC 1. Confirm the drift is present in the raw data.
# MAGIC 2. Create a **Time series** monitor in the UI (steps below).
# MAGIC 3. Refresh the monitor and read the drift on the dashboard / metric tables.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The story in the data
# MAGIC
# MAGIC Around **day 22** a sensor recalibration shifts measured `launch_speed` up ~4 mph,
# MAGIC and the model rolls from `caught_v1` to `caught_v2`, which is not calibrated for the
# MAGIC shifted inputs. On **day 28** a bad batch lands with null `hang_time` and
# MAGIC out-of-range (negative) `launch_angle`.
# MAGIC
# MAGIC | Signal | What moves |
# MAGIC |---|---|
# MAGIC | Feature drift | `launch_speed` mean ~88 to ~92 mph |
# MAGIC | Prediction drift | `predicted_caught` rate ~0.76 to ~0.91 |
# MAGIC | Data quality | `hang_time` null spike, `launch_angle` goes negative |
# MAGIC
# MAGIC The SQL cells use unqualified table names. `00_config` set the current catalog and
# MAGIC schema, so `caught_in_air_inference` resolves to your target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM caught_in_air_inference LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Confirm the drift before we monitor
# MAGIC
# MAGIC These show, by day, what the monitor will detect automatically. Run them first so
# MAGIC you can point at the change, then say "the monitor finds this for us."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature and prediction drift by day
# MAGIC Watch `avg_launch_speed` and `pred_catch_rate` step up when `caught_v2` takes over.
# MAGIC `actual_catch_rate` stays flat, so the model is drifting away from reality.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc('day', event_timestamp)      AS day,
# MAGIC        model_version,
# MAGIC        count(*)                                AS n,
# MAGIC        round(avg(launch_speed), 1)             AS avg_launch_speed,
# MAGIC        round(avg(predicted_caught), 3)         AS pred_catch_rate,
# MAGIC        round(avg(actual_caught), 3)            AS actual_catch_rate
# MAGIC FROM caught_in_air_inference
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data-quality issues by day
# MAGIC The bad batch shows up as a null spike in `hang_time` and negative `launch_angle`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc('day', event_timestamp)                        AS day,
# MAGIC        count(*)                                                  AS n,
# MAGIC        sum(CASE WHEN hang_time IS NULL THEN 1 ELSE 0 END)        AS hang_time_nulls,
# MAGIC        sum(CASE WHEN launch_angle < 0 THEN 1 ELSE 0 END)         AS bad_launch_angle
# MAGIC FROM caught_in_air_inference
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the Time series monitor in the UI
# MAGIC
# MAGIC Time series is the profile type for drift over time. It computes profile stats per
# MAGIC time window and drift metrics comparing each window to the one before it.
# MAGIC
# MAGIC 1. **Catalog** -> your catalog -> your schema -> `caught_in_air_inference`.
# MAGIC 2. Open the **Quality** tab, then **Create monitor** (older workspaces: "Monitoring" / "Get started").
# MAGIC 3. **Profile type:** Time series.
# MAGIC 4. **Timestamp column:** `event_timestamp`.
# MAGIC 5. **Granularities:** `1 day` (optionally add `1 week`).
# MAGIC 6. **Slicing expressions (optional):** `model_version`, `fielder_position`, `stadium`.
# MAGIC 7. **Output:** leave default. It writes `caught_in_air_inference_profile_metrics`
# MAGIC    and `caught_in_air_inference_drift_metrics` into the same schema.
# MAGIC 8. **Dashboard SQL warehouse:** pick a warehouse.
# MAGIC 9. **Schedule:** manual is fine for a demo, or set daily.
# MAGIC 10. **Create**, then **Refresh** metrics. The first refresh backfills all 35 days,
# MAGIC     so give it a few minutes until the status is **Active / succeeded**.
# MAGIC
# MAGIC > Time series drift is **consecutive-window** by default (each day vs the prior day),
# MAGIC > so no baseline table is needed. To compare every window against a fixed reference,
# MAGIC > set a **baseline table** in the monitor config instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read the drift after the refresh
# MAGIC
# MAGIC Open **View dashboard** from the Quality tab and look at:
# MAGIC - `launch_speed` numeric drift (mean / percentile shift at the change point)
# MAGIC - `predicted_caught` and `caught_probability` (distribution shift from over-prediction)
# MAGIC - `hang_time` percent nulls (spike on the bad-batch day) and `launch_angle` min going negative
# MAGIC - categorical drift on `fielder_position` and `stadium` if you sliced on them
# MAGIC
# MAGIC The cells below query the generated metric tables directly. **They only work after
# MAGIC the monitor has been created and refreshed once.** Exact column names can vary by
# MAGIC workspace / monitor version; adjust if a column is not found.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profile metrics (per column, per window)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Runs only after the monitor has refreshed once.
# MAGIC SELECT window, column_name, count, percent_null, avg, min, max
# MAGIC FROM caught_in_air_inference_profile_metrics
# MAGIC WHERE column_name IN ('launch_speed', 'predicted_caught', 'hang_time', 'launch_angle')
# MAGIC   AND log_type = 'INPUT'
# MAGIC ORDER BY column_name, window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drift metrics (window vs the previous window)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Runs only after the monitor has refreshed once.
# MAGIC SELECT window, column_name, drift_type,
# MAGIC        js_distance,                               -- numeric distribution drift
# MAGIC        chi_squared_test.pvalue AS chi_sq_pvalue   -- categorical drift
# MAGIC FROM caught_in_air_inference_drift_metrics
# MAGIC WHERE column_name IN ('launch_speed', 'predicted_caught', 'caught_probability', 'fielder_position')
# MAGIC ORDER BY column_name, window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tear down / rebuild
# MAGIC
# MAGIC - **Rebuild the table:** re-run `01_generate_caught_in_air` (it overwrites the table).
# MAGIC - **Tear down:** delete the monitor from the table's **Quality** tab (this also removes
# MAGIC   its metric tables), then drop the table:
# MAGIC
# MAGIC   ```sql
# MAGIC   DROP TABLE IF EXISTS caught_in_air_inference;
# MAGIC   ```
# MAGIC
# MAGIC If you rebuild after creating a monitor, delete and recreate the monitor (or refresh
# MAGIC it) so its metric history matches the fresh data.
