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

    df.write.format("jdbc").mode("append").option("driver", db_config.driver).option(
        "url", db_config.url
    ).option("dbtable", db_config.table).option("user", db_config.user).option(
        "password", db_config.password
    ).save()

    spark.stop()
