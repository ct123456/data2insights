from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import sys
sys.path.insert(0,"~/ubuntu/data2insights/src")
from src.models.spark_config import SparkConfig

dag = DAG(
    "census_dag",
    description="DAG to load census data into PostgreSQL",
    schedule_interval="0 12 * * *",
    start_date=datetime(2019, 9, 26),
    catchup=False,
)

convert_census_to_parquet_command_spark_config = SparkConfig("convert_census_to_parquet.py")
convert_census_to_parquet = BashOperator(
    task_id="convert_census_to_parquet",
    bash_command=convert_census_to_parquet_command_spark_config.bash_command,
    retries=1,
    dag=dag,
)

clean_zipcode_census_spark_config = SparkConfig("clean_census_zipcode.py")
clean_zipcode_census = BashOperator(
    task_id="convert_census_to_parquet",
    bash_command=clean_zipcode_census_spark_config.bash_command,
    retries=1,
    dag=dag,
)

write_census_to_db_spark_config = SparkConfig("write_census_to_db.py")
write_census_to_db = BashOperator(
    task_id="convert_census_to_parquet",
    bash_command=write_census_to_db_spark_config.bash_command,
    retries=1,
    dag=dag,
)

convert_census_to_parquet >> clean_zipcode_census >> write_census_to_db
