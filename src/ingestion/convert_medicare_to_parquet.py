from pyspark.sql import SparkSession, SQLContext
import lib.utils as Utils
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder.appName("blah").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/Medicare/raw/*.csv"
    s3_output_file_location = "s3a://data2insights/Medicare/parquet/medicare_physicians_and_other_suppliers_2012_2015"

    Utils.convert_csvs_to_parquet(
        spark, s3_input_file_location, s3_output_file_location
    )

    spark.stop()
