import pyspark.sql.functions as F
from src.lib.zip_utils import get_zip5


def convert_csvs_to_parquet(spark, s3_input, s3_output, schema=None):
    dataframe = read_csvs_from_s3(spark, s3_input, schema)
    cleaned_col_df = clean_columns(dataframe)
    write_df_to_s3(cleaned_col_df, s3_output)


def read_csvs_from_s3(spark, s3_path_files_location, schema=None):

    if schema is not None:
        dataframe = (
            spark.read.format("csv")
            .option("inferSchema", False)
            .schema(schema)
            .option("header", True)
            .load(s3_path_files_location)
        )
    else:
        dataframe = (
            spark.read.format("csv")
            .option("inferSchema", True)
            .option("header", True)
            .load(s3_path_files_location)
        )

    return dataframe


def write_df_to_s3(dataframe, s3_path_file_location):
    dataframe.write.save(s3_path_file_location, format="parquet", mode="overwrite")


def clean_columns(dataframe):
    for column_name in dataframe.columns:
        new_name = (
            column_name.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "")
        )
        dataframe = dataframe.withColumnRenamed(column_name, new_name)
    return dataframe


def rename_columns(dataframe, rename_list):
    for col in rename_list:
        dataframe.withColumnRenamed(F.col(col.name), col.new_name)
    return dataframe


def add_zip5_col(dataframe, column_name="zipcode"):
    get_zip5_udf = F.udf(get_zip5, F.StringType())
    return dataframe.withColumn("zip5", get_zip5_udf(dataframe[column_name]))
