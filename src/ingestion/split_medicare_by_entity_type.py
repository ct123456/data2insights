from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/Medicare/parquet/medicare_clean_zipcode"

    s3_hcp_output_file_location = "s3a://data2insights/Medicare/parquet/medicare_hcp"
    s3_hco_output_file_location = "s3a://data2insights/Medicare/parquet/medicare_hco"

    medicare_df = sqlContext.read.parquet(s3_input_file_location)

    medicare_hcp_df = medicare_df.where(medicare_df.nppes_entity_code == 'I').coalesce(8)
    medicare_hco_df = medicare_df.where(medicare_df.nppes_entity_code == 'O').coalesce(8)

    Utils.write_df_to_s3(medicare_hcp_df, s3_hcp_output_file_location)
    Utils.write_df_to_s3(medicare_hco_df, s3_hco_output_file_location)

    spark.stop()
