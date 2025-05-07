# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_date,current_timestamp, input_file_name,avg,sum

# COMMAND ----------

sales_constrains ={
    'Remove_Missing_PromoCode' : 'promo_code IS NOT NULL',
    'Remove_Less_Price' : 'price > 15'
}

# COMMAND ----------

source_path = '/Volumes/practice/testschema/sales/'
@dlt.table(
    comment="Bronze table - Incrementally loads new files with Auto Loader. Includes Ingestion Date and Filename."
)
@dlt.expect_all(sales_constrains)
def bronze_sales():
    return spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')\
            .load(source_path)\
                .withColumn('ingestion_date', current_date())\
                    .withColumn('filename', col("_metadata.file_path"))


# COMMAND ----------

@dlt.table(
    comment="Silver table - Cleaned sales data."
)
def silver_sales():
    bronze = dlt.read_stream('bronze_sales')
    return bronze.withColumn('Quantity', col('Quantity').cast('int'))\
        .withColumn('price', col('price').cast('double'))\
            .withColumn('order_date', col('order_date').cast('date'))

# COMMAND ----------

@dlt.table(
    comment="Gold table - Customer level total sales."
)
def gold_sales():
    silver = dlt.read('silver_sales')
    return silver.groupBy('product_id').agg(avg('price').alias('avg_price'), avg('Quantity').alias('avg_quantity'))
