from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
import src.lib.utils as Utils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    s3_npi_counts_file_location = "s3a://data2insights/zipcode/parquet/zipcode_from_npi"
    s3_uszips_file_location = "s3a://data2insights/uszips/parquet/uszips"
    s3_medicare_count_file_location = "s3a://data2insights/zipcode/parquet/medicare_count_by_zipcode"

    s3_output_file_location = "s3a://data2insights/zipcode/parquet/combined_neighborhood_master"

    df_npi_counts = sqlContext.read.parquet(s3_npi_counts_file_location)
    df_medicare_counts = sqlContext.read.parquet(s3_medicare_count_file_location)
    df_uszips = sqlContext.read.parquet(s3_uszips_file_location).withColumnRenamed(
        "zip", "zip5"
    )

    df_hcp = df_npi_counts.where(df_npi_counts.entity_type_code == 1).select(
        F.col("zip5"), F.col("count").alias("hcp_count")
    )
    df_hco = df_npi_counts.where(df_npi_counts.entity_type_code == 2).select(
        F.col("zip5"), F.col("count").alias("hco_count")
    )

    df_zip_counts = df_hcp.join(df_hco, ["zip5"], how="full")

    cols = [
        F.col("zip5").alias("zip_code"),
        F.col("state_id").alias("state"),
        F.col("lat").alias("latitude"),
        F.col("lng").alias("longitude"),
        F.col("county_fips").alias("county_fips"),
        F.col("county_name").alias("county_name"),
        F.col("hcp_count").alias("provider_count"),
        F.col("hco_count").alias("institution_count"),
        F.col("zipcode_medicare_count").alias("medicare_count")
    ]

    df_neighborhood = df_zip_counts.join(df_uszips, ["zip5"], how="full").join(df_medicare_counts, ["zip5"], how="full").select(cols)

    Utils.write_df_to_s3(df_neighborhood, s3_output_file_location)

    spark.stop()
