from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from src.models.spark_config import SparkConfig

dag = DAG(
    "uszips",
    description="DAG to load uszipz into PostgreSQL",
    start_date=datetime(2019, 9, 26),
    catchup=False
)

convert_uszips_to_parquet_spark_config = SparkConfig("convert_uszips_to_parquet.py")
convert_uszips_to_parquet = BashOperator(
    task_id="convert_uszips_to_parquet",
    bash_command=convert_uszips_to_parquet.bash_command,
    retries=1,
    dag=dag
)

write_uszips_to_db_spark_config = SparkConfig("write_uszips_to_db.py")
write_uszips_to_db = BashOperator(
    task_id="write_uszips_to_db",
    bash_command=write_uszips_to_db.bash_command,
    retries=1,
    dag=dag
)

