from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare2", db_table="public.institution")

    s3_input_file_location = "s3a://data2insights/institution/parquet/scored_institution_master"

    cols = [
        F.col("npi").alias("npi"),
        F.col("name").alias("name"),
        F.col("medicare_count").alias("medicare_count"),
        F.col("score").alias("score"),
        F.col("zip_code").alias("zip_code")
    ]

    scored_institution_df = sqlContext.read.parquet(s3_input_file_location).select(cols)

    DbUtils.insert(db_config=db_config, dataframe=scored_institution_df)

    spark.stop()
