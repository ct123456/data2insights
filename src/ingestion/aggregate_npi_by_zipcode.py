from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = (
        "s3a://data2insights/NPPES/parquet/npi_subset_clean_zipcode"
    )
    s3_output_file_location = "s3a://data2insights/zipcode/parquet/zipcode_from_npi"

    npi_df = sqlContext.read.parquet(s3_input_file_location)

    hcp_counts = (
        npi_df.where(npi_df.entity_type_code == 1)
        .groupBy("zip5")
        .agg(F.countDistinct("npi").alias("hcp_count"))
    )
    hco_counts = (
        npi_df.where(npi_df.entity_type_code == 2)
        .groupBy("zip5")
        .agg(F.countDistinct("npi").alias("hco_count"))
    )

    zipcode_df = hcp_counts.join(hco_counts, ["zip5"], how="full")
    Utils.write_df_to_s3(zipcode_df, s3_output_file_location)

    spark.stop()
