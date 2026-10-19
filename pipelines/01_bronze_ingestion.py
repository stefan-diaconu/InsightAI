# Databricks notebook source

print("InsightAI - NHS Data Engineering")

# Check that Apache Spark is working
print("Spark version:", spark.version)


# COMMAND ----------


# Path to our NHS data file
file_path = "/Volumes/workspace/default/nhs_raw/Monthly-AE-March-2026-revised-flkg42.csv"

# Check the first 1,000 characters of the raw file
print(dbutils.fs.head(file_path, 1000))


# COMMAND ----------


# Read the raw NHS CSV into a Spark DataFrame
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(file_path)
)

# Check the structure of our dataset
print("Number of rows:", df.count())
print("Number of columns:", len(df.columns))

# Display the first 5 records
display(df.limit(5))


# COMMAND ----------


spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

print("Bronze schema created!")


# COMMAND ----------


from pyspark.sql.functions import lit, current_timestamp

# Add information about where the data came from
bronze_df = (
    df
    .withColumn("_source_file", lit(file_path))
    .withColumn("_ingested_at", current_timestamp())
)

# Save our first Delta table
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable("workspace.bronze.ae_monthly")
)

print("Bronze table created!")


# COMMAND ----------


bronze = spark.table("workspace.bronze.ae_monthly")

print("Bronze rows:", bronze.count())
print("Bronze columns:", len(bronze.columns))

display(bronze.limit(5))


# COMMAND ----------

