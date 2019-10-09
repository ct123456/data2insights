from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    npi_hcp_s3_input_file_location = "s3a://data2insights/NPPES/parquet/npi_hcp"
    medicare_hcp_s3_input_file_location = "s3a://data2insights/provider/parquet/medicare_count_by_hcp"

    s3_output_file_location = "s3a://data2insights/provider/parquet/combined_provider_master"

    limit = 10000000
    cols = [
        F.col("npi").alias("npi"),
        F.col("provider_last_name_legal_name").alias("last_name"),
        F.col("provider_first_name").alias("first_name"),
        F.col("provider_middle_name").alias("middle_name"),
        F.col("provider_name_suffix_text").alias("suffix"),
        F.col("provider_credential_text").alias("credentials"),
        F.col("provider_gender_code").alias("gender"),
        F.col("healthcare_provider_taxonomy_code_1").alias("specialty"),
        F.col("hcp_medicare_count").alias("medicare_count"),
        F.col("zip5").alias("zip_code")
    ]

    npi_hcp_df = sqlContext.read.parquet(npi_hcp_s3_input_file_location)
    medicare_hcp_df = sqlContext.read.parquet(medicare_hcp_s3_input_file_location)

    provider_df = npi_hcp_df.join(medicare_hcp_df, ["npi"], how="left").select(cols).limit(limit)

    Utils.write_df_to_s3(provider_df, s3_output_file_location)

    spark.stop()
