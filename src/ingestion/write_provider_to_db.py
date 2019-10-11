from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare2", db_table="public.provider")

    s3_input_file_location = "s3a://data2insights/provider/parquet/scored_provider_master"

    cols = [
        F.col("npi").alias("npi"),
        F.col("last_name").alias("last_name"),
        F.col("first_name").alias("first_name"),
        F.col("middle_name").alias("middle_name"),
        F.col("suffix").alias("suffix"),
        F.col("credentials").alias("credentials"),
        F.col("gender").alias("gender"),
        F.col("specialty").alias("specialty"),
        F.col("medicare_count").alias("medicare_count"),
        F.col("score").alias("score"),
        F.col("zip_code").alias("zip_code")
    ]

    scored_hcp_df = sqlContext.read.parquet(s3_input_file_location).select(cols)

    DbUtils.insert(db_config=db_config, dataframe=scored_hcp_df)

    spark.stop()
