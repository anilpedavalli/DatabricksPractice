# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, col


# COMMAND ----------

source_path = '/Volumes/practice/testschema/Customers/'
@dlt.view(
    comment="Bronze table - Incrementally loads new files with Auto Loader. Includes Ingestion Date and Filename."
)
def bronze_sales_vw():
    return spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
            .load(source_path)\
                .withColumn('ingestion_date', current_timestamp())\
                    .withColumn('filename', col("_metadata.file_path"))


# COMMAND ----------

dlt.create_streaming_table("Customer_Bronze_Sales")

#SCD Type2
dlt.apply_changes(
    target = "Customer_Bronze_Sales",
    source = "bronze_sales_vw",
    keys = ["customer_id"],
    sequence_by = col("ingestion_date"),
    stored_as_scd_type = 2
)

# COMMAND ----------


