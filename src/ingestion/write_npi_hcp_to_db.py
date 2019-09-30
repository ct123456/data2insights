from pyspark.sql import SparkSession, SQLContext
from src.models.db_config import DbConfig
import src.lib.db_utils as DbUtils

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()
    sqlContext = SQLContext(spark)

    db_config = DbConfig(db_name="healthcare", db_table="public.npi_hcp")

    s3_input_file_location = (
        "s3a://data2insights/NPPES/parquet/npi_hcp"
    )

    limit = 100000000
    cols = ["npi", "entity_type_code", "provider_last_name_legal_name", "provider_first_name", "provider_middle_name", "provider_name_suffix_text", "provider_credential_text", "provider_first_line_business_mailing_address" , "provider_second_line_business_mailing_address", "provider_business_mailing_address_city_name", "provider_business_mailing_address_state_name", "zip5", "provider_gender_code", "healthcare_provider_taxonomy_code_1"]
    df = sqlContext.read.parquet(s3_input_file_location).select(cols).limit(limit)
    DbUtils.insert(db_config=db_config, dataframe=df)

    spark.stop()