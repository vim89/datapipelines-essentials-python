import time
import unittest

from imports.HdfsImport import HdfsImport
from mapper.Mapper import ComplexDataMapper
from utils.Utilities import SparkSettings

start_time = time.time()


class XmlMapperTest(unittest.TestCase):
    def test2_CreateDDL_Views(self):
        print("Testing HdfsImport readFromSource")
        self.sparksettings = SparkSettings("XmlMapperTest")
        self.spark = self.sparksettings.getSparkSession()
        self.hdfsImport = HdfsImport(self.spark)

        # Read JSON file from given path
        json_df = self.spark.read.json(path='resources/ct_gov/trial_20200324104240.json')

        # Register as temporary view / table for flattening queries to execute on
        json_df.createOrReplaceTempView('xmltable')

        # Create an object of class XmlMapper from Mapper.py by passing spark variable
        xml_mapper: ComplexDataMapper = ComplexDataMapper(sc=self.spark)

        # Call createViews function by passing json_df dataframe, it returns 2 things flattening queries and XPATH (
        # Only for XML; Ignore for JSON)
        view_queries = xml_mapper.createViews(df=json_df)

        # Loop through all queries, execute them, physicalize flattened attributes as table - Repeat steps to all
        # queries (Nested attributes)
        for q in view_queries[0]:
            print(f'Executing query:'
                  f'{view_queries[0][q]}')
            temp_df = self.spark.sql(view_queries[0][q])
            temp_df.createOrReplaceTempView(q)
            select_cols = []
            for col in temp_df.schema.fields:
                if not str(col.dataType).lower().startswith("struct") and not str(col.dataType).lower().startswith(
                        "array"):
                    select_cols.append(col.name)
            temp_df.select(select_cols).show()


if __name__ == '__main__':
    unittest.main()
