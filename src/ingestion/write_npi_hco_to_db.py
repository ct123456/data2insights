from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare", db_table="public.npi_hco")

    s3_input_file_location = (
        "s3a://data2insights/NPPES/parquet/npi_hco"
    )

    limit = 10000000
    cols = ["npi", "entity_type_code", "provider_organization_name_legal_business_name", "zip5"]
    df = sqlContext.read.parquet(s3_input_file_location).select(["npi", "entity_type_code", "provider_organization_name_legal_business_name"]).limit(limit)
    DbUtils.insert(db_config=db_config, dataframe=df)

    spark.stop()