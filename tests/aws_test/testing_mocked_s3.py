import os
import signal
import subprocess
import unittest

import boto3
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class MockTestGlueJob(unittest.TestCase):
    # start moto server, by default it runs on localhost on port 5000.
    process = subprocess.Popen(
        ['moto_server', 's3'],
        stdout=subprocess.PIPE,
        shell=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
    )

    @classmethod
    def setUpClass(cls) -> None:
        # create an s3 connection that points to the moto server.
        s3_conn = boto3.resource(
            "s3", endpoint_url="http://127.0.0.1:5000"
        )
        # create an S3 bucket.
        s3_conn.create_bucket(Bucket="bucket")
        # # configure pyspark to use hadoop-aws module. os.environ[ "PYSPARK_SUBMIT_ARGS" ] = '--packages
        # "org.apache.hadoop:hadoop-aws:2.7.3" --packages "org.apache.httpcomponents:httpclient:4.2.5" ' \
        # '--packages "org.xerial.snappy:snappy-java:1.1.7.3" pyspark-shell '

    def test_s3_glue_jobs_locally(self):
        # get the spark session object and hadoop configuration.
        spark = SparkSession.builder.getOrCreate()
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        # mock the aws credentials to access s3.
        hadoop_conf.set("fs.s3a.access.key", "dummy-value")
        hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
        # we point s3a to our moto server.
        hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
        # we need to configure hadoop to use s3a.
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # create a pyspark dataframe.
        values = [("k1", 1), ("k2", 2)]
        columns = ["key", "value"]
        df = spark.createDataFrame(values, columns)
        # write the dataframe as csv to s3.
        df.write.mode('overwrite').csv("s3://bucket/source.csv")
        # read the dataset from s3
        df = spark.read.csv("s3://bucket/source.csv")
        # print Data
        df.show()
        # assert df is a DataFrame
        assert isinstance(df, DataFrame)
        print("test_s3_glue_jobs_locally successfully completed")

    @classmethod
    def tearDownClass(cls) -> None:
        # shut down the moto server.
        os.kill(cls.process.pid, signal.SIGTERM)


if __name__ == "__main__":
    try:
        unittest.main()
    except Exception:
        MockTestGlueJob().tearDownClass()
