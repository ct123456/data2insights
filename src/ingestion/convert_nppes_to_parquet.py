from pyspark.sql import SparkSession, SQLContext
import lib.utils as Utils
from pyspark.sql.types import *


if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = (
        "s3a://data2insights/NPPES/raw/npidata_pfile_20050523-20190908.csv"
    )
    s3_output_file_location = "s3a://data2insights/NPPES/parquet/npi"

    Utils.convert_csvs_to_parquet(
        spark, s3_input_file_location, s3_output_file_location
    )

    spark.stop()
