from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import lib.utils as Utils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/Medicare/raw/*.csv"
    s3_output_file_location = "s3a://data2insights/Medicare/parquet/medicare_physicians_and_other_suppliers_2012_2015"

    Utils.convert_csvs_to_parquet(
        spark, s3_input_file_location, s3_output_file_location
    )

    spark.stop()
