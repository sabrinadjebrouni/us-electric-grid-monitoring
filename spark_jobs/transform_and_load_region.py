import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sha2, concat_ws
from pyspark.sql import functions as F



if __name__ == "__main__":

    #date_path = sys.argv[1]  # must be format YYYY/MM/DD
    project_id = sys.argv[1]
    bucket_name = sys.argv[2]
    dataset_id = sys.argv[3]
    
    # Create a SparkSession (entry point to Spark functionality)
    spark = SparkSession.builder \
        .appName('transforming eia region data') \
        .getOrCreate()

    #df_region = spark.read.parquet(f"gs://{bucket_name}/region_data/{date_path}/*.parquet")
    df_region = spark.read.parquet(f"gs://{bucket_name}/region_data/*/*/*/*.parquet")

    df_region_formatted = df_region \
        .withColumn("period", to_timestamp(col("period"), "yyyy-MM-dd'T'HH")) \
        .withColumnRenamed("respondent-name", "respondent_name") \
        .withColumnRenamed("type-name", "type_name") \
        .withColumn("value", col("value").cast("int"))\
        .withColumnRenamed("value-units", "value_units")

    # Cleaning rows with missing values for fuel_type
    null_rows_count = df_region_formatted.filter(
        col("value").isNull() | 
        col("period").isNull() | 
        col("respondent").isNull() | 
        col("type").isNull()
    ).count()

    df_region_cleaned = df_region_formatted.na.drop(subset=["value", "period", "respondent", "type"])

    # Adding unique ID to each row
    df_region_hashed = df_region_cleaned.withColumn('id', 
        sha2(concat_ws("||", col('period'), col('respondent'), col('type'), col('value')), 256)) \
        .select('id', 'period', 'respondent', 'respondent_name', 'type', 'type_name', 'value', 'value_units')

    #Cleaning duplicates rows
    try:
        df_existing_ids = spark.read.format("bigquery") \
            .option("query", f"SELECT id FROM `{project_id}.{dataset_id}.region`") \
            .load()
        
        # Keep only rows that are not in BigQuery already
        df_region_final = df_region_hashed.join(df_existing_ids, "id", "left_anti")
    except:
        # If table doesn't exist yet, just use cleaned data
        df_region_final = df_region_hashed

    # Write dataframe into bigquery
    df_region_final.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .option("writeAtLeastOnce", "true")\
    .mode("append") \
    .save(f"{project_id}.{dataset_id}.region")

    # Closing the Spark session
    spark.stop()