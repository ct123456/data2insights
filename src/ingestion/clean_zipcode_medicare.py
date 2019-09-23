from pyspark.sql import SparkSession, SQLContext
import lib.utils as Utils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = (
        "s3a://data2insights/Medicare/parquet/medicare_subset_10000"
    )
    s3_output_file_location = (
        "s3a://data2insights/Medicare/parquet/medicare_subset_clean_zipcode"
    )

    medicare_subset_df = sqlContext.read.parquet(s3_input_file_location)
    medicare_subset_clean_zipcode_df = Utils.add_zip5_col(
        medicare_subset_df, "nppes_provider_zip"
    )
    Utils.write_df_to_s3(medicare_subset_clean_zipcode_df, s3_output_file_location)

    spark.stop()
