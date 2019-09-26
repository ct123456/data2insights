import os


class SparkConfig(object):

    def __init__(self, script_name):
        master_node_ip = os.environ['MASTER_NODE_IP']
        base_path = "~/data2insights"

        self.py_file_path = "{base_path}/builds/dependencies.zip".format(base_path=base_path)
        self.master_spark_node = "spark://{master_node_ip}:7077".format(master_node_ip=master_node_ip)
        self.jar_path = "{base_path}/jars/postgresql-42.2.8.jar".format(base_path=base_path)
        self.script_path = "{base_path}/src/ingestion/{script_name}".format(script_name=script_name)

        self.bash_command = "spark-submit " \
                            "--py-files {py_file_path} " \
                            "--master {master_spark_node} " \
                            "--conf spark.executor.memory=6g " \
                            "--jars {jar_path} {script_path}".format(
            py_file_path=self.py_file_path,
            master_spark_node=self.master_spark_node,
            jar_path=self.jar_path,
            script_path=self.script_path
        )

