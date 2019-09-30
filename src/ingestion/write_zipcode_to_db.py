from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare", db_table="public.zipcode")

    s3_input_file_location = "s3a://data2insights/zipcode/parquet/zipcode_from_npi"

    df = sqlContext.read.parquet(s3_input_file_location)

    df_hcp = df.where(df.entity_type_code == 1).select(F.col("zip5"), F.col("count").alias("hcp_count"))
    df_hco = df.where(df.entity_type_code == 2).select(F.col("zip5"), F.col("count").alias("hco_count"))
    df_zip = df_hcp.join(df_hco, ["zip5"], how="full")
    DbUtils.insert(db_config=db_config, dataframe=df_zip)

    spark.stop()