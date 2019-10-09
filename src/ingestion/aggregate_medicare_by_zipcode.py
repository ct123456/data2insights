from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    zipcode_s3_input_file_location = "s3a://data2insights/Medicare/parquet/medicare_clean_zipcode"
    zipcode_s3_output_file_location = "s3a://data2insights/zipcode/parquet/medicare_count_by_zipcode"

    medicare_df = sqlContext.read.parquet(zipcode_s3_input_file_location)

    zipcode_counts_df = (
        medicare_df.groupBy("zip5")
        .agg(F.count(F.lit(1)).alias("zipcode_medicare_count"))
    )

    Utils.write_df_to_s3(zipcode_counts_df, zipcode_s3_output_file_location)

    spark.stop()
