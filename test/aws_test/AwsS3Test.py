import os
import signal
import subprocess
import unittest

import boto3
from pyspark.python.pyspark.shell import sc as spark_shell
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from utils.Utilities import download_files_to_s3, zip_extract_filter


class AwsS3Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # create an s3 connection that points to the moto server.
        cls.s3_resource_obj = boto3.resource(
            "s3", endpoint_url="http://127.0.0.1:5000"
        )

        cls.s3_client_obj = boto3.client(
            "s3",
            endpoint_url="http://127.0.0.1:5000"
        )
        # create an S3 bucket.
        cls.s3_resource_obj.create_bucket(Bucket="bucket")
        # start moto server, by default it runs on localhost on port 5000.
        cls.process = subprocess.Popen(
            ['moto_server', 's3'],
            stdout=subprocess.PIPE,
            shell=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )

        # # configure pyspark to use hadoop-aws module. os.environ[ "PYSPARK_SUBMIT_ARGS" ] = '--packages
        # "org.apache.hadoop:hadoop-aws:2.7.3" --packages "org.apache.httpcomponents:httpclient:4.2.5" ' \
        # '--packages "org.xerial.snappy:snappy-java:1.1.7.3" pyspark-shell '

        # get the spark session object and hadoop configuration.
        cls.spark: SparkSession = SparkSession.builder.getOrCreate()
        cls.hadoop_conf = cls.spark.sparkContext._jsc.hadoopConfiguration()
        # mock the aws credentials to access s3.
        cls.hadoop_conf.set("fs.s3a.access.key", "dummy-value")
        cls.hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
        # we point s3a to our moto server.
        cls.hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
        # we need to configure hadoop to use s3a.
        cls.hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    @classmethod
    def test_dataframe_operation_s3(cls):
        # create a pyspark dataframe.
        values = [("k1", 1), ("k2", 2)]
        columns = ["key", "value"]
        df = cls.spark.createDataFrame(values, columns)
        # write the dataframe as csv to s3.
        df.write.mode('overwrite').csv("s3://bucket/source.csv")
        # read the dataset from s3
        df = cls.spark.read.csv("s3://bucket/source.csv")
        # print Data
        df.show()
        # assert df is a DataFrame
        assert isinstance(df, DataFrame)
        print("test_s3_glue_jobs_locally successfully completed")

    @classmethod
    def test_download_files_to_s3(cls):
        download_files_to_s3(s3_client_obj=cls.s3_client_obj,
                             download_file_url='https://github.com/vim89/covid19/archive/master.zip',
                             target_filename='clinical_study.zip', bucket_name="bucket")
        zip_file: RDD = spark_shell.binaryFiles("s3://bucket/clinical_study.zip")
        file_names_rdd: RDD = zip_file.map(
            lambda f: zip_extract_filter(f, filter_condition='ends_with', filter_pattern='.py'))
        print(file_names_rdd.take(10))

    @classmethod
    def tearDownClass(cls) -> None:
        # shut down the moto server.
        os.kill(cls.process.pid, signal.SIGTERM)


if __name__ == '__main__':
    unittest.main()
