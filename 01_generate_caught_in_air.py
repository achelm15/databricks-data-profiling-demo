# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Generate the synthetic inference table
# MAGIC
# MAGIC Builds `<catalog>.<schema>.caught_in_air_inference` (~120k rows over 35 days) for a
# MAGIC synthetic "caught in air" model. No model is trained. The catch probabilities are
# MAGIC synthesized directly so the demo can focus on profiling and drift.
# MAGIC
# MAGIC The table carries a deliberate drift story:
# MAGIC - **day ~22:** a sensor recalibration shifts `launch_speed` up ~4 mph, and the model
# MAGIC   rolls from `caught_v1` to `caught_v2` (miscalibrated, over-predicts catches)
# MAGIC - **day ~28:** a bad batch with null `hang_time` and out-of-range `launch_angle`
# MAGIC
# MAGIC Tune the constants (`N`, `DAYS`, `CHANGE_DAY`, `BAD_DAY`) to reshape the story.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json
from pyspark.sql import functions as F

# CATALOG, SCHEMA, TABLE come from 00_config
N = 120000
DAYS = 35
CHANGE_DAY = 22   # sensor recalibration + model v2 rollout
BAD_DAY = 28      # data-quality bad batch

# COMMAND ----------

df = spark.range(0, N, numPartitions=16)

# --- time + regime ---
df = df.withColumn("day_idx", F.floor(F.rand() * F.lit(DAYS)).cast("int"))
df = df.withColumn("regime", F.col("day_idx") >= F.lit(CHANGE_DAY))
df = df.withColumn(
    "event_timestamp",
    F.expr(
        f"(current_timestamp() - make_interval(0,0,0,{DAYS},0,0,0)) "
        "+ make_interval(0,0,0, day_idx, cast(floor(rand()*24) as int), cast(floor(rand()*60) as int), 0)"
    ),
)

# --- raw features ---
df = df.withColumn("launch_angle_raw", F.lit(25.0) + F.randn() * F.lit(12.0))
df = df.withColumn(
    "base_speed",
    F.greatest(F.lit(50.0), F.least(F.lit(120.0), F.lit(88.0) + F.randn() * F.lit(12.0))),
)
# sensor recalibration shifts measured launch speed up ~4 mph after CHANGE_DAY
df = df.withColumn("sensor_shift", F.when(F.col("regime"), F.lit(4.0)).otherwise(F.lit(0.0)))
df = df.withColumn("launch_speed", F.round(F.least(F.lit(122.0), F.col("base_speed") + F.col("sensor_shift")), 1))
df = df.withColumn("spray_angle", F.round(F.greatest(F.lit(-50.0), F.least(F.lit(50.0), F.randn() * F.lit(20.0))), 1))
df = df.withColumn(
    "hang_time_true",
    F.greatest(F.lit(1.0), F.least(F.lit(7.0),
        F.lit(0.06) * F.col("launch_angle_raw") + F.lit(0.015) * F.col("base_speed") + F.randn() * F.lit(0.4))),
)
df = df.withColumn(
    "hit_distance",
    F.round(F.greatest(F.lit(50.0), F.least(F.lit(450.0),
        (F.col("base_speed") - F.lit(40.0)) * F.lit(3.2) + F.col("launch_angle_raw") * F.lit(1.5) + F.randn() * F.lit(25.0))), 0),
)

# fielder position (weighted, OF-heavy for air balls)
df = df.withColumn("_rp", F.rand())
df = df.withColumn(
    "fielder_position",
    F.when(F.col("_rp") < 0.22, "CF").when(F.col("_rp") < 0.42, "LF").when(F.col("_rp") < 0.62, "RF")
     .when(F.col("_rp") < 0.72, "SS").when(F.col("_rp") < 0.80, "2B").when(F.col("_rp") < 0.87, "3B")
     .when(F.col("_rp") < 0.94, "1B").when(F.col("_rp") < 0.97, "P").otherwise("C"),
)

# stadium (weighted)
df = df.withColumn("_rs", F.rand())
df = df.withColumn(
    "stadium",
    F.when(F.col("_rs") < 0.40, "Coors Field").when(F.col("_rs") < 0.55, "Dodger Stadium")
     .when(F.col("_rs") < 0.70, "Chase Field").when(F.col("_rs") < 0.85, "Oracle Park")
     .when(F.col("_rs") < 0.95, "Petco Park").otherwise("Great American Ball Park"),
)

df = df.withColumn(
    "fielder_start_distance",
    F.round(F.least(F.lit(130.0), F.exp(F.lit(3.2) + F.randn() * F.lit(0.5))), 1),
)

# --- true "makeable" play + actual outcome (sharp, so a calibrated model scores high) ---
df = df.withColumn(
    "logit_true",
    F.lit(0.5) + F.lit(1.4) * (F.col("hang_time_true") - F.lit(2.8)) - F.lit(0.09) * (F.col("fielder_start_distance") - F.lit(26.0)),
)
df = df.withColumn("true_prob", F.lit(1.0) / (F.lit(1.0) + F.exp(-F.col("logit_true"))))
df = df.withColumn("hard_label", (F.col("logit_true") >= F.lit(0.0)).cast("int"))
# 5% irreducible label noise
df = df.withColumn(
    "actual_caught",
    F.when(F.rand() < F.lit(0.05), F.lit(1) - F.col("hard_label")).otherwise(F.col("hard_label")),
)

# --- model score: v1 calibrated; v2 miscalibrated and over-predicts catches ---
df = df.withColumn("model_bias", F.when(F.col("regime"), F.lit(1.2)).otherwise(F.lit(0.0)))
df = df.withColumn("model_logit", F.col("logit_true") + F.col("model_bias") + F.randn() * F.lit(0.10))
df = df.withColumn("caught_probability", F.round(F.lit(1.0) / (F.lit(1.0) + F.exp(-F.col("model_logit"))), 4))
df = df.withColumn("predicted_caught", (F.col("caught_probability") >= F.lit(0.5)).cast("int"))
df = df.withColumn("model_version", F.when(F.col("regime"), F.lit("caught_v2")).otherwise(F.lit("caught_v1")))

# --- data-quality bad batch on BAD_DAY: null hang_time + out-of-range launch_angle ---
df = df.withColumn("_bad", F.col("day_idx") == F.lit(BAD_DAY))
df = df.withColumn(
    "hang_time",
    F.when(F.col("_bad") & (F.rand() < F.lit(0.4)), F.lit(None).cast("double")).otherwise(F.round(F.col("hang_time_true"), 2)),
)
df = df.withColumn(
    "launch_angle",
    F.when(F.col("_bad") & (F.rand() < F.lit(0.10)), F.lit(-15.0)).otherwise(F.round(F.col("launch_angle_raw"), 1)),
)
df = df.withColumn("play_id", F.concat(F.lit("PLAY-"), F.lpad(F.col("id").cast("string"), 7, "0")))

final = df.select(
    "play_id", "event_timestamp", "model_version",
    "launch_speed", "launch_angle", "hang_time", "spray_angle", "hit_distance",
    "fielder_position", "fielder_start_distance", "stadium",
    "caught_probability", "predicted_caught", "actual_caught",
)

final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
print(f"Wrote {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

by_ver = spark.sql(f"""
  SELECT model_version,
         count(*) AS rows,
         round(avg(predicted_caught),3) AS pred_catch_rate,
         round(avg(actual_caught),3)    AS actual_catch_rate,
         round(avg(CASE WHEN predicted_caught=actual_caught THEN 1 ELSE 0 END),3) AS accuracy,
         round(avg(launch_speed),1)     AS avg_launch_speed
  FROM {TABLE} GROUP BY model_version ORDER BY model_version
""")
display(by_ver)

# COMMAND ----------

display(spark.sql(f"""
  SELECT
    (SELECT count(*) FROM {TABLE} WHERE hang_time IS NULL)   AS hang_time_nulls,
    (SELECT count(*) FROM {TABLE} WHERE launch_angle < 0)    AS neg_launch_angle,
    (SELECT min(event_timestamp) FROM {TABLE})               AS min_ts,
    (SELECT max(event_timestamp) FROM {TABLE})               AS max_ts
"""))
