import getpass
import unittest

from utils.Utilities import create_spark_session, count_words, split_words
from utils.transformation_extension import *


class UtilsTest(unittest.TestCase):

    def test1_testSparkSettings(self):
        print("Testing Spark Settings")
        self.spark = create_spark_session(application_name="Utils Test")
        self.assertEqual(str(self.spark.version), "2.4.5")
        self.assertEqual(str(self.spark.sparkContext.sparkUser()), getpass.getuser())

        metadf: DataFrame = self.spark.read.option("header", "true").format("csv").load(path="resources/meta.csv")

        self.assertEqual(True, True)

    def test2_test_custom_transformations(self):
        print("Testing Environment")
        self.spark = create_spark_session(application_name="Utils Test")
        line_array = ["Hello,World,How,are,you", "Hello.World.How.are.you", "Hello;World;How;are;you",
                      "Hello-World-How-are-you", "Hello|World|How|are|you", "Hello World How are you"]

        lines_rdd: RDD[str] = self.spark.sparkContext.parallelize(line_array)
        df = lines_rdd.transform(lambda _rdd: split_words(_rdd)).transform(lambda _rdd: count_words(_rdd))
        df.toDF().toDF("Word", "Count").show()

        self.assertTrue(df is not None)
        self.assertEqual(df.count(), 5)


if __name__ == '__main__':
    unittest.main()
