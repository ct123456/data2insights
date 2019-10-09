from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    npi_hco_s3_input_file_location = "s3a://data2insights/NPPES/parquet/npi_hco"
    medicare_hco_s3_input_file_location = "s3a://data2insights/institution/parquet/medicare_count_by_hco"

    s3_output_file_location = "s3a://data2insights/institution/parquet/combined_institution_master"

    limit = 10000000
    cols = [
        F.col("npi").alias("npi"),
        F.col("provider_organization_name_legal_business_name").alias("name"),
        F.col("hco_medicare_count").alias("medicare_count"),
        F.col("zip5").alias("zip_code")
    ]

    npi_hco_df = sqlContext.read.parquet(npi_hco_s3_input_file_location)
    medicare_hco_df = sqlContext.read.parquet(medicare_hco_s3_input_file_location)

    institution_df = npi_hco_df.join(medicare_hco_df, ["npi"], how="left").select(cols).limit(limit)

    Utils.write_df_to_s3(institution_df, s3_output_file_location)

    spark.stop()
