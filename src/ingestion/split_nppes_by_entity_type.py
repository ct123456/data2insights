from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/NPPES/parquet/npi_subset_10000"

    s3_hcp_output_file_location = "s3a://data2insights/NPPES/parquet/npi_hcp_subset"
    s3_hco_output_file_location = "s3a://data2insights/NPPES/parquet/npi_hco_subset"

    npi_subset_df = sqlContext.read.parquet(s3_input_file_location)

    npi_hcp_subset_df = npi_subset_df.where(npi_subset_df.entity_type_code == 1)
    npi_hco_subset_df = npi_subset_df.where(npi_subset_df.entity_type_code == 2)

    Utils.write_df_to_s3(npi_hcp_subset_df, s3_hcp_output_file_location)
    Utils.write_df_to_s3(npi_hco_subset_df, s3_hco_output_file_location)

    spark.stop()
