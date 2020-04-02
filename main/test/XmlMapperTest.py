import time
import unittest

from main.src.imports.HdfsImport import HdfsImport
from main.src.utils.Utilities import SparkSettings
from mapper.Mapper import XmlMapper

start_time = time.time()


class XmlMapperTest(unittest.TestCase):
    def test2_CreateDDL_Views(self):
        print("Testing HdfsImport readFromSource")
        self.sparksettings = SparkSettings("XmlMapperTest")
        self.spark = self.sparksettings.getSparkSession()
        self.hdfsImport = HdfsImport(self.spark)

        # Read JSON file from given path
        json_df = self.spark.read.json(path='resources/epic/lilly')

        # Register as temporary view / table for flattening queries to execute on
        json_df.createOrReplaceTempView('xmltable')

        # Create an object of class XmlMapper from Mapper.py by passing spark variable
        xml_mapper: XmlMapper = XmlMapper(sc=self.spark)

        # Call createViews function by passing json_df dataframe, it returns 2 things flattening queries and XPATH (Only for XML; Ignore for JSON)
        view_queries = xml_mapper.createViews(df=json_df)

        # Loop through all queries, execute them, physicalize flattened attributes as table - Repeat steps to all queries (Nested attributes)
        for q in view_queries[0]:
            print(f'Executing query:'
                  f'{view_queries[0][q]}')
            try:
                temp_df = self.spark.sql(view_queries[0][q])
                temp_df.createOrReplaceTempView(q)
                select_cols = []
                for col in temp_df.schema.fields:
                    if not str(col.dataType).lower().startswith("struct") and not str(col.dataType).lower().startswith(
                            "array"):
                        select_cols.append(col.name)
                temp_df.select(select_cols).show()
                # temp_df.select(select_cols).coalesce(1).write.csv(f"{q}")
            except Exception:
                print(f'Query {q} failed to execute')


if __name__ == '__main__':
    unittest.main()
