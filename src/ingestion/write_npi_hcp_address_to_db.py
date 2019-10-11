from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils
import pyspark.sql.functions as F

if __name__ == "__main__":
    NPI_SOURCE_ID = 1

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare2", db_table="public.npi_hcp_address")

    s3_input_file_location = (
        "s3a://data2insights/NPPES/parquet/npi_hcp"
    )

    limit = 10
    cols = [
        F.col("npi").alias("npi"),
        F.col("provider_first_line_business_mailing_address").alias("address1"),
        F.col("provider_second_line_business_mailing_address").alias("address2"),
        F.col("provider_business_mailing_address_city_name").alias("city"),
        F.col("provider_business_mailing_address_state_name").alias("state"),
        F.col("zip5").alias("zip_code"),
        F.lit(NPI_SOURCE_ID).alias("source_id")
    ]

    df = sqlContext.read.parquet(s3_input_file_location).select(cols).limit(limit)
    DbUtils.insert(db_config=db_config, dataframe=df)

    spark.stop()
