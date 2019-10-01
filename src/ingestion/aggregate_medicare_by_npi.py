from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    hcp_s3_input_file_location = "s3a://data2insights/Medicare/parquet/medicare_hcp"
    hco_s3_input_file_location = "s3a://data2insights/Medicare/parquet/medicare_hco"

    hcp_s3_output_file_location = "s3a://data2insights/provider/parquet/medicare_count_by_hcp"
    hco_s3_output_file_location = "s3a://data2insights/institution/parquet/medicare_count_by_hco"

    medicare_hcp_df = sqlContext.read.parquet(hcp_s3_input_file_location)
    medicare_hco_df = sqlContext.read.parquet(hco_s3_input_file_location)

    hcp_counts_df = (
        medicare_hcp_df.groupBy("npi")
        .agg(F.count("npi").alias("hcp_medicare_count"))
    )

    hco_counts_df = (
        medicare_hco_df.groupBy("npi")
        .agg(F.count.alias("hco_medicare_count"))
    )

    Utils.write_df_to_s3(hcp_counts_df, hcp_s3_output_file_location)
    Utils.write_df_to_s3(hco_counts_df, hco_s3_output_file_location)

    spark.stop()
