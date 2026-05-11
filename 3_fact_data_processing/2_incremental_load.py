# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/sumahegde.work@outlook.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "databricksmaster", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog       = dbutils.widgets.get("catalog")
data_source   = dbutils.widgets.get("data_source")

# COMMAND ----------

base_path    = f"abfss://source@myownstoragesm.dfs.core.windows.net/processed/child/{data_source}/"
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table   = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

print("Base Path:", base_path)
print("Bronze:", bronze_table)
print("Silver:", silver_table)
print("Gold:", gold_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE

# COMMAND ----------

# ── BRONZE — Read from ADLS ───────────────────────────────
df = (
    spark.read
        .options(header=True, inferSchema=True)
        .format("csv")
        .option("recursiveFileLookup", "true")
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .withColumn("file_name", F.expr("_metadata.file_path"))
        .withColumn("file_modification_time", F.expr("_metadata.file_modification_time"))
)

print("Total Rows:", df.count())
df.show(5)

# COMMAND ----------

# ── BRONZE — Watermark Incremental Load ──────────────────
try:
    last_watermark = spark.sql(
        f"SELECT MAX(file_modification_time) AS max_ts FROM {bronze_table}"
    ).collect()[0]["max_ts"]
    print(f"Last watermark: {last_watermark}")
except:
    last_watermark = None
    print("No watermark — full load")

# Filter only new files
if last_watermark:
    df = df.filter(F.col("file_modification_time") > F.lit(last_watermark))
    print(f"New records after filter: {df.count()}")

# Write to Bronze
df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .saveAsTable(bronze_table)

print("Bronze done")

# COMMAND ----------

# MAGIC %md
# MAGIC ##SILVER

# COMMAND ----------

# ── SILVER — Read from Bronze ─────────────────────────────
df_orders = spark.sql(f"SELECT * FROM {bronze_table}")

# COMMAND ----------

df_orders.show(2)

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) FROM {bronze_table}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------



# 1. Keep only rows where order_qty is present
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. Clean customer_id → keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. Remove weekday name from the date text
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse date — multiple formats (corrected: F.to_date instead of F.try_to_date)
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.to_date("order_placement_date", "yyyy/MM/dd"),
        F.to_date("order_placement_date", "dd-MM-yyyy"),
        F.to_date("order_placement_date", "dd/MM/yyyy"),
        F.to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(
    ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]
)

# 6. Cast product_id to string
df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

print("Silver records:", df_orders.count())
df_orders.show(5)

# COMMAND ----------

# Drop Bronze metadata columns — not needed in Silver
df_orders = df_orders.drop("read_timestamp", "file_name", "file_modification_time")

# COMMAND ----------

# ── SILVER — Join with products dimension ────────────────
df_products = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.products")
df_orders = df_orders.join(df_products, on="product_id", how="left")

# COMMAND ----------

# Deduplicate before MERGE — keep latest record per order_id + product_id
from pyspark.sql.window import Window
window = Window.partitionBy("order_id", "product_id").orderBy(F.col("order_placement_date").desc())
df_orders = df_orders.withColumn("rn", F.row_number().over(window)) \
                     .filter(F.col("rn") == 1) \
                     .drop("rn")

# COMMAND ----------


# ── SILVER — Write with MERGE ─────────────────────────────
if spark.catalog.tableExists(silver_table):
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("target").merge(
        df_orders.alias("source"),
        "target.order_id = source.order_id AND target.product_id = source.product_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    df_orders.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(silver_table)

print(" Silver done")

# COMMAND ----------

# MAGIC %md
# MAGIC ##GOLD

# COMMAND ----------

# ── GOLD CHILD — Build fact table ────────────────────────
df_gold = spark.sql(f"""
    SELECT
        order_id,
        order_placement_date AS date,
        customer_id          AS customer_code,
        product_code,
        product_id,
        order_qty            AS sold_quantity
    FROM {silver_table}
""")


# COMMAND ----------

df_gold.count()

# COMMAND ----------

# Deduplicate before Gold MERGE
window_gold = Window.partitionBy("order_id", "product_code", "customer_code").orderBy(F.col("date").desc())
df_gold = df_gold.withColumn("rn", F.row_number().over(window_gold)) \
                 .filter(F.col("rn") == 1) \
                 .drop("rn")

# COMMAND ----------


# Write or MERGE gold child table
if spark.catalog.tableExists(gold_table):
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(
        df_gold.alias("gold"),
        """source.date = gold.date
        AND source.order_id = gold.order_id
        AND source.product_code = gold.product_code
        AND source.customer_code = gold.customer_code"""
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_gold.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .mode("overwrite") \
        .saveAsTable(gold_table)

print(" Gold done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging with Parent company

# COMMAND ----------

# MAGIC %md
# MAGIC - Note: We want data for monthly level but child data is on daily level

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental Load**

# COMMAND ----------

 # ── MERGE CHILD GOLD → PARENT GOLD ───────────────────────
# Child data is daily, parent expects monthly aggregation

# Step 1: Find which months are in new child data
incremental_month_df = df_gold.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------


# Step 2: Get all child rows for those months from Gold
monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {gold_table} sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
""")


# COMMAND ----------

print("Total Rows: ", monthly_table.count())
monthly_table.show(10)

# COMMAND ----------

# Step 3: Aggregate daily → monthly level
df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)

print("Monthly recalc rows:", df_monthly_recalc.count())
df_monthly_recalc.show(5)

# COMMAND ----------


# Step 4: Merge into Parent Gold
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("parent_gold").merge(
    df_monthly_recalc.alias("child_gold"),
    """parent_gold.date = child_gold.date
    AND parent_gold.product_code = child_gold.product_code
    AND parent_gold.customer_code = child_gold.customer_code"""
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print("Child merged into Parent Gold successfully")

# COMMAND ----------

# df_parent_gold.agg(
#     F.min("date").alias("earliest_date"),
#     F.max("date").alias("latest_date"),
#     F.countDistinct("product_code").alias("unique_products"),
#     F.countDistinct("customer_code").alias("unique_customers")
# ).show()

# COMMAND ----------

spark.sql("SELECT * FROM databricksmaster.gold.fact_orders LIMIT 10").show()