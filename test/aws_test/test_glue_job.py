import os
import signal
import subprocess
import unittest
from unittest import mock

import boto3
from pyspark.sql import SparkSession

from aws_test import glue_job


class TestGlueJob(unittest.TestCase):
    """
    This test class setup a test environment to test our glue job,
    runs the glue job and checks the result.
    """

    @classmethod
    def setUpClass(cls):
        """
        the setup class starts a moto server, creates an S3 bucket,
        configures PySpark and Spark and dumps the source dataframe to S3.
        """
        S3_MOCK_ENDPOINT = "http://127.0.0.1:5000"

        # setup moto server
        # cls.process = subprocess.Popen(
        # "moto_server s3", stdout=subprocess.PIPE,
        # shell=True, preexec_fn=os.setsid()
        # )

        cls.process = subprocess.Popen(
            ['moto_server', 's3'],
            stdout=subprocess.PIPE,
            shell=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )

        # create s3 connection, bucket and s3 url's
        cls.s3_conn = boto3.resource(
            "s3", region_name="eu-central-1",
            endpoint_url=S3_MOCK_ENDPOINT
        )
        bucket = "bucket"
        cls.s3_conn.create_bucket(Bucket=bucket)
        cls.s3_source = "s3://{}/{}".format(bucket, "source.csv")
        cls.s3_destination = "s3://{}/{}".format(bucket, "destination.csv")

        # Setup spark to use s3, and point it to the moto server.
        os.environ[
            "PYSPARK_SUBMIT_ARGS"
        ] = """--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell"""
        cls.spark = SparkSession.builder.getOrCreate()
        hadoop_conf = cls.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.access.key", "mock")
        hadoop_conf.set("fs.s3a.secret.key", "mock")
        hadoop_conf.set("fs.s3a.endpoint", S3_MOCK_ENDPOINT)

        # create source dataframe and write the dataframe as csv to s3
        values = [("k1", 1), ("k2", 2)]
        columns = ["key", "value"]
        df = cls.spark.createDataFrame(values, columns)
        df.write.csv(cls.s3_source)

    @mock.patch("glue_job._commit_job")
    @mock.patch("glue_job._get_glue_args")
    @mock.patch("glue_job._get_spark_session_and_glue_job")
    def test_glue_job_runs_successfully(self, m_session_job, m_get_glue_args, m_commit):
        """
        we arrange our test function; construct the arguments that we get from the cli, set the return
        values of our mocked functions.
        we run our glue job and assert if the result is what we expect.
        """
        # arrange
        cli_args = {"source": self.s3_source, "destination": self.s3_destination}

        m_session_job.return_value = self.spark, None
        m_get_glue_args.return_value = cli_args

        # act
        glue_job.run(cli_args=cli_args, spark=self.spark)

        # assert
        df = self.spark.read.csv(self.s3_destination)
        self.assertTrue(not df.rdd.isEmpty())

    @classmethod
    def tearDownClass(cls):
        # shut down moto server
        os.killpg(os.getpgid(cls.process.pid), signal.SIGTERM)


if __name__ == "__main__":
    try:
        unittest.main()
    except Exception:
        TestGlueJob().tearDownClass()
