from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare2", db_table="public.medicare")

    s3_input_file_location = (
        "s3a://data2insights/Medicare/parquet/medicare_clean_zipcode"
    )

    limit = 10000000
    cols = [
        F.col("npi").alias("npi"),
        F.col("hcpcs_code").alias("hcpcs_code"),
        F.col("hcpcs_description").alias("hcpcs_description"),
        F.col("line_srvc_cnt").alias("line_service_count"),
        F.col("zip5").alias("zip_code"),
    ]

    df = sqlContext.read.parquet(s3_input_file_location).select(cols).limit(limit)
    DbUtils.insert(db_config=db_config, dataframe=df)

    spark.stop()
