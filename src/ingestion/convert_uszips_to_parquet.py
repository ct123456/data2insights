from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    schema = StructType(
        [
            StructField("zip", StringType(), True),
            StructField("lat", StringType(), True),
            StructField("lng", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_id", StringType(), True),
            StructField("state_name", StringType(), True),
            StructField("zcta", BooleanType(), True),
            StructField("parent_zcta", BooleanType(), True),
            StructField("population", IntegerType(), True),
            StructField("density", DoubleType(), True),
            StructField("county_fips", StringType(), True),
            StructField("county_name", StringType(), True),
            StructField("all_county_weights", StringType(), True),
            StructField("imprecise", BooleanType(), True),
            StructField("military", BooleanType(), True),
            StructField("timezone", StringType(), True)
        ]
    )

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/uszips/raw/*.csv"
    s3_output_file_location = "s3a://data2insights/uszips/parquet/uszips"

    Utils.convert_csvs_to_parquet(
        spark, s3_input_file_location, s3_output_file_location, schema
    )

    spark.stop()
