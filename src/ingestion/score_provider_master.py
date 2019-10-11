from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/provider/parquet/combined_provider_master"
    s3_output_file_location = "s3a://data2insights/provider/parquet/scored_provider_master"

    combined_provider_master_df = sqlContext.read.parquet(s3_input_file_location)
    scored_provider_df = combined_provider_master_df.withColumn("score", F.col("medicare_count") / 511.0 * 100)

    Utils.write_df_to_s3(scored_provider_df, s3_output_file_location)

    spark.stop()
