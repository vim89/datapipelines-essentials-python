from pyspark.sql import *

from objects.enums.Environments import Environment, Environments


# ToDo - Any other util functions if required
class SparkSettings(object):
    def __init__(self, appName):
        self.appName = appName
        self.environment: Environment = Environments().getEnvironmentByServer()

    def getSparkSession(self):
        try:
            # ============================
            # This is the part where spark object is created
            # ============================
            spark = SparkSession.builder \
                .appName(self.appName) \
                .enableHiveSupport() \
                .getOrCreate()
            # spark.conf.set("spark.executor.cores", no_of_cores)
            # spark.conf.set("spark.executor.memory", executor_memory)
            # spark.conf.set("spark.submit.deployMode", deploy_mode)
            spark.conf.set("spark.yarn.historyServer.address", self.environment.historyserver)
            spark.conf.set("spark.sql.crossJoin.enabled", "true")
            spark.conf.set("spark.sql.optimizer.maxIterations", "5000")
            spark.conf.set("spark.eventLog.dir", "hdfs:///spark2-history")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
            spark.conf.set("spark.shuffle.compress", "true")
            spark.conf.set("spark.io.compression.codec", "snappy")
            return spark
        except Exception:
            raise
