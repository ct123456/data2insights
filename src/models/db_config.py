import os


class DbConfig(object):

    def __init__(self, db_name, db_table):
        self.driver = "org.postgresql.Driver"
        self.host = os.environ['DB_HOST']
        self.user = os.environ['DB_USER']
        self.password = os.environ['DB_PASSWORD']
        self.table = db_table

        self.url = "jdbc:postgresql://{db_host}/{db_name}".format(db_host=self.db_host, db_name=db_name)

    @property
    def properties(self):
        return {
            "driver": self.driver,
            "user": self.user,
            "password": self.password
        }
