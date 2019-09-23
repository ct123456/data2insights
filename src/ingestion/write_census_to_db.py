from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare", db_table="public.census")

    s3_input_file_location = (
        "s3a://data2insights/Census/parquet/2010census_clean_zipcode"
    )
    
    df = sqlContext.read.parquet(s3_input_file_location)
    df.write.mode("append").jdbc(db_config.url, db_config.table, db_config.properties)

    spark.stop()
