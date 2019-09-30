def insert(db_config, dataframe):

    dataframe.repartition(8).write.format("jdbc").mode("append").option(
        "driver", db_config.driver
    ).option("url", db_config.url).option("dbtable", db_config.table).option(
        "user", db_config.user
    ).option(
        "password", db_config.password
    ).option(
        "batchsize", 100000
    ).option(
        "numPartitions", 9
    ).save()
