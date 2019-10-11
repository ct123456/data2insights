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

convert_npi_to_parquet_command_spark_config = SparkConfig(script_name="convert_nppes_to_parquet.py")
convert_npi_to_parquet = BashOperator(
    task_id="convert_npi_to_parquet",
    bash_command=convert_npi_to_parquet_command_spark_config.bash_command,
    retries=1,
    dag=dag
)

clean_zipcode_npi_spark_config = SparkConfig("clean_zipcode_npi.py")
clean_zipcode_npi = BashOperator(
    task_id="clean_zipcode_npi",
    bash_command=clean_zipcode_npi_spark_config.bash_command,
    retries=1,
    dag=dag
)

split_nppes_by_entity_type_spark_config = SparkConfig("split_nppes_by_entity_type.py")
split_nppes_by_entity_type = BashOperator(
    task_id="split_nppes_by_entity_type",
    bash_command=split_nppes_by_entity_type_spark_config.bash_command,
    retries=1,
    dag=dag
)

write_npi_hcp_to_db_spark_config = SparkConfig("write_npi_hcp_to_db.py")
write_npi_hcp_to_db = BashOperator(
    task_id="write_npi_hcp_to_db",
    bash_command=write_npi_hcp_to_db_spark_config.bash_command,
    retries=1,
    dag=dag
)

write_npi_hco_to_db_spark_config = SparkConfig("write_npi_hco_to_db.py")
write_npi_hco_to_db = BashOperator(
    task_id="write_npi_hco_to_db",
    bash_command=write_npi_hco_to_db_spark_config.bash_command,
    retries=1,
    dag=dag
)

aggregate_npi_by_zipcode_spark_config = SparkConfig("aggregate_npi_by_zipcode.py")
aggregate_npi_by_zipcode = BashOperator(
    task_id="aggregate_npi_by_zipcode",
    bash_command=aggregate_npi_by_zipcode_spark_config.bash_command,
    retries=1,
    dag=dag
)

write_zipcode_to_db_spark_config = SparkConfig("write_zipcode_to_db.py")
write_zipcode_to_db = BashOperator(
    task_id="write_zipcode_to_db",
    bash_command=write_zipcode_to_db_spark_config.bash_command,
    retries=1,
    dag=dag
)

convert_npi_to_parquet >> clean_zipcode_npi >> split_nppes_by_entity_type >> write_npi_hcp_to_db
split_nppes_by_entity_type.set_downstream(write_npi_hco_to_db)
split_nppes_by_entity_type.set_downstream(aggregate_npi_by_zipcode)
aggregate_npi_by_zipcode.set_downstream(write_zipcode_to_db)
