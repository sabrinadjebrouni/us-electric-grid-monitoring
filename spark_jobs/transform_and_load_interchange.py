import sys,os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sha2, concat_ws
from pyspark.sql import functions as F



if __name__ == "__main__":

    date_path = sys.argv[1]  # must be format YYYY/MM/DD
    project_id = sys.argv[2]
    bucket_name = sys.argv[3]
    dataset_id = sys.argv[4]

    # Create a SparkSession (entry point to Spark functionality)
    spark = SparkSession.builder \
        .appName('transforming eia interchange data') \
        .getOrCreate()


    df_interchange = spark.read.parquet(f"gs://{bucket_name}/interchange_data/{date_path}/*.parquet")


    df_interchange_formatted = df_interchange \
        .withColumn("period", to_timestamp(col("period"), "yyyy-MM-dd'T'HH")) \
        .withColumnRenamed("fromba-name", "fromba_name") \
        .withColumnRenamed("toba-name", "toba_name") \
        .withColumn("value", col("value").cast("int"))\
        .withColumnRenamed("value-units", "value_units")


    # Cleaning rows with missing values for interchange
    null_rows_count = df_interchange_formatted.filter(
        col("value").isNull() | 
        col("period").isNull() | 
        col("fromba").isNull() | 
        col("toba").isNull()
    ).count()

    df_interchange_cleaned = df_interchange_formatted.na.drop(subset=["value", "period", "fromba", "toba"])

    # Adding unique ID to each row
    df_interchange_hashed = df_interchange_cleaned.withColumn('id', 
        sha2(concat_ws("||", col('period'), col('fromba'), col('toba'), col('value')), 256)) \
        .select('id', 'period', 'fromba', 'fromba_name', 'toba', 'toba_name', 'value', 'value_units')

        #Cleaning duplicates rows
    try:
        df_existing_ids = spark.read.format("bigquery") \
            .option("query", f"SELECT id FROM `{project_id}.{dataset_id}.intechange`") \
            .load()
        
        # Keep only rows that are not in BigQuery already
        df_interchange_final = df_interchange_hashed.join(df_existing_ids, "id", "left_anti")
    except:
        # If table doesn't exist yet, just use cleaned data
        df_interchange_final = df_interchange_hashed

    # Write dataframe into bigquery
    df_interchange_final.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .option("writeAtLeastOnce", "true")\
    .mode("append") \
    .save(f"{project_id}.{dataset_id}.interchange")

    # Closing the Spark session
    spark.stop()