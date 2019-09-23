from pyspark.sql import SparkSession, SQLContext
import src.lib.utils as Utils
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession.builder.appName("data2insights").getOrCreate()

    sqlContext = SQLContext(spark)

    s3_input_file_location = "s3a://data2insights/NPPES/parquet/npi_subset_clean_zipcode"
    s3_output_file_location = "s3a://data2insights/zipcode/parquet/zipcode_from_npi"

    npi_df = sqlContext.read.parquet(s3_input_file_location)
    zipcode_df = (
        npi_df.select(["npi", "entity_type_code", "zip5"])
        .groupBy("zip5", "entity_type_id")
        .agg(F.countDistinct("npi").alias("count"))
    )
    Utils.write_df_to_s3(zipcode_df, s3_output_file_location)

    spark.stop()
