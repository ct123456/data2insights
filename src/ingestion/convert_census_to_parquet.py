from pyspark.sql import SparkSession, SQLContext
import lib.utils as Utils
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    schema = StructType(
        [
            StructField("geo_id", StringType(), True),
            StructField("zipcode", StringType(), True),
            StructField("population", IntegerType(), True),
            StructField("minimum_age", IntegerType(), True),
            StructField("maximum_age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ]
    )

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/Census/raw/*.csv"
    s3_output_file_location = "s3a://data2insights/Census/parquet/2010census"

    Utils.convert_csvs_to_parquet(
        spark, s3_input_file_location, s3_output_file_location, schema
    )

    spark.stop()
