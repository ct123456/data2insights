from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare2", db_table="public.neighborhood")

    s3_input_file_location = "s3a://data2insights/zipcode/parquet/scored_neighborhood_master"

    cols = [
        F.col("zip_code").alias("zip_code"),
        F.col("state").alias("state"),
        F.col("latitude").alias("latitude"),
        F.col("longitude").alias("longitude"),
        F.col("county_fips").alias("county_fips"),
        F.col("county_name").alias("county_name"),
        F.col("provider_count").alias("provider_count"),
        F.col("institution_count").alias("institution_count"),
        F.col("medicare_count").alias("medicare_count"),
        F.col("score").alias("score")
    ]

    scored_neighborhood_df = sqlContext.read.parquet(s3_input_file_location).select(cols)
    DbUtils.insert(db_config=db_config, dataframe=scored_neighborhood_df)

    spark.stop()
