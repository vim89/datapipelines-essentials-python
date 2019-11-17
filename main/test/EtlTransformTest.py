import time
import unittest

from etl.ETLTransform import Transform
from etl.meta import MetaModel
from etl.meta.MetaModel import *
from utils.Utilities import SparkSettings

start_time = time.time()


class MyTestCase(unittest.TestCase):
    def testEtlTransformations(self):
        self.spark = SparkSettings("EtlTransformTest").getSparkSession()
        metamodel = MetaModel(datamodelpath='resources/datamodel.csv', sc=self.spark)

        # print(f"Data model as JSON -> \n{metamodel.datamodel}")

        metamodel.readMetadataFromCsv(sc=self.spark, metadatapath='resources/meta.csv', targettable='invoice')
        metamodel.readSourceFilesIntoDF()

        targetddl = metamodel.getTargetDdl('PARQUET', True)
        # print('------Target DDL ------')
        # print('------Target Query ------')
        # print(f"{queryhead} {querytail}")

        # self.spark.sql(f"{queryhead} {querytail}").show()

        trans = Transform(targettable='invoice', model=metamodel, sc=self.spark)

        trans.transform()
        self.assertIsNotNone(trans)


if __name__ == '__main__':
    unittest.main()
