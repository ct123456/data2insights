from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from src.models.spark_config import SparkConfig

dag = DAG(
    "medicare_dag",
    description="DAG to load medicare data into PostgreSQL",
    start_date=datetime(2019, 9, 26),
    catchup=False,
)

convert_medicare_to_parquet_command_spark_config = SparkConfig("convert_medicare_to_parquet.py")
convert_medicare_to_parquet = BashOperator(
    task_id="convert_medicare_to_parquet",
    bash_command=convert_medicare_to_parquet_command_spark_config.bash_command,
    retries=1,
    dag=dag,
)

clean_zipcode_medicare_spark_config = SparkConfig("clean_zipcode_medicare.py")
clean_zipcode_medicare = BashOperator(
    task_id="clean_zipcode_medicare",
    bash_command=clean_zipcode_medicare_spark_config.bash_command,
    retries=1,
    dag=dag,
)

write_medicare_to_db_spark_config = SparkConfig("write_medicare_to_db.py")
write_medicare_to_db = BashOperator(
    task_id="write_medicare_to_db",
    bash_command=write_medicare_to_db_spark_config.bash_command,
    retries=1,
    dag=dag,
)

convert_medicare_to_parquet >> clean_zipcode_medicare >> write_medicare_to_db