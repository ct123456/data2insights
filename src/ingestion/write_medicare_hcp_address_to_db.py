from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils
import pyspark.sql.functions as F

if __name__ == "__main__":

    MEDICARE_SOURCE_ID = 2

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare2", db_table="public.provider_address")

    s3_input_file_location = (
        "s3a://data2insights/Medicare/parquet/medicare_hcp"
    )

    limit = 10000000
    cols = [
        F.col("npi").alias("npi"),
        F.col("nppes_provider_street1").alias("address1"),
        F.col("nppes_provider_street2").alias("address2"),
        F.col("nppes_provider_city").alias("city"),
        F.col("nppes_provider_state").alias("state"),
        F.col("zip5").alias("zip_code"),
        F.lit(MEDICARE_SOURCE_ID).alias("source_id")
    ]

    df = sqlContext.read.parquet(s3_input_file_location).select(cols).limit(limit)
    DbUtils.insert(db_config=db_config, dataframe=df)

    spark.stop()
