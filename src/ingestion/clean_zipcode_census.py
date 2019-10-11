from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/Census/parquet/2010census"
    s3_output_file_location = (
        "s3a://data2insights/Census/parquet/2010census_clean_zipcode"
    )

    census_2010_df = sqlContext.read.parquet(s3_input_file_location)
    census_2010_clean_zipcode_df = Utils.add_zip5_col(census_2010_df)
    Utils.write_df_to_s3(census_2010_clean_zipcode_df, s3_output_file_location)

    spark.stop()
