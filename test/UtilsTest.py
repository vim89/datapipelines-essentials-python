import string
import unittest

from join_sample import transformSplitExplodeField
from pyspark.sql import DataFrame

from utils.Utilities import SparkSettings


class UtilsTest(unittest.TestCase):

    def test1_testSparkSettings(self):
        print("Testing Spark Settings")
        self.sparksettings = SparkSettings("UtilsTest")
        self.spark = self.sparksettings.getSparkSession()
        self.assertEqual(str(self.spark.version), "2.3.4")
        self.assertEqual(str(self.spark.sparkContext.sparkUser()).lower(), "vimirji")

        metadf: DataFrame = self.spark.read.option("header", "true").format("csv").load(path="resources/meta.csv")

        self.assertEqual(True, True)

    def test2_testEnvironment(self):
        print("Testing Environment")
        self.sparksettings = SparkSettings("UtilsTest")
        self.spark = self.sparksettings.getSparkSession()
        self.assertEqual(self.sparksettings.environment.name, "local")

        data = [(1, '|'.join(string.ascii_lowercase), 'Tavneet')]

        df = self.spark.createDataFrame(data).toDF('id', 'categorylist', 'name')
        print('Input dataframe')
        df.show()

        newdf: DataFrame = transformSplitExplodeField(sc=self.spark, df=df, splitfield='categorylist',
                                                      splitseperator='|')

        print('Output Dataframe')
        newdf.show(100)


if __name__ == '__main__':
    unittest.main()
