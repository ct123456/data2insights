from pyspark.sql import SparkSession, SQLContext
import lib.utils as Utils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/NPPES/parquet/npi"
    s3_output_file_location = "s3a://data2insights/NPPES/parquet/npi_subset_10000"

    medicare_df = sqlContext.read.parquet(s3_input_file_location)
    medicare_subset_df = medicare_df.limit(10000)
    Utils.write_df_to_s3(medicare_subset_df, s3_output_file_location)

    spark.stop()
