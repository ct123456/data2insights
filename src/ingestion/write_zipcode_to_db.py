from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare", db_table="public.zipcode")

    s3_input_file_location = "s3a://data2insights/zipcode/parquet/zipcode_from_npi"

    df = sqlContext.read.parquet(s3_input_file_location)
    DbUtils.insert(db_config=db_config, dataframe=df)

    spark.stop()
