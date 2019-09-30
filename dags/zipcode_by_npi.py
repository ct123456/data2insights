from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from src.models.spark_config import SparkConfig

dag = DAG(
    "npi_dag",
    description="DAG to load npi data into PostgreSQL",
    start_date=datetime(2019, 9, 26),
    catchup=False
)

aggregate_npi_hcp_by_zipcode_spark_config = SparkConfig("aggregate_npi_by_zipcode.py")
aggregate_npi_hcp_by_zipcode = BashOperator(
    task_id="aggregate_npi_hcp_by_zipcode",
    bash_command=aggregate_npi_hcp_by_zipcode_spark_config.bash_command,
    retries=1,
    dag=dag
)

write_zipcode_to_db_spark_config = SparkConfig("write_zipcode_to_db.py")
write_zipcode_to_db = BashOperator(
    task_id="write_npi_hco_to_db",
    bash_command=write_zipcode_to_db_spark_config.bash_command,
    retries=1,
    dag=dag
)

