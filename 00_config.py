# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Config
# MAGIC
# MAGIC Central config for the demo. The other notebooks start with `%run ./00_config`
# MAGIC to pick up the target catalog and schema, so this is the only place you set them.
# MAGIC
# MAGIC Set the **catalog** and **schema** widgets at the top of the notebook (or change the
# MAGIC defaults below), then run. This notebook:
# MAGIC - reads the `catalog` / `schema` widgets into `CATALOG`, `SCHEMA`, `TABLE`
# MAGIC - creates the schema if needed
# MAGIC - sets the session's current catalog and schema, so SQL cells elsewhere can use
# MAGIC   unqualified table names

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dqm", "Schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
TABLE = f"{CATALOG}.{SCHEMA}.caught_in_air_inference"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"CATALOG = {CATALOG}")
print(f"SCHEMA  = {SCHEMA}")
print(f"TABLE   = {TABLE}")
