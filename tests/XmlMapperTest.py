import unittest

from com.vitthalmirji.imports.HdfsImport import HdfsImport
from com.vitthalmirji.mapper.Mapper import ComplexDataMapper


class XmlMapperTest(unittest.TestCase):
    def test_create_hive_ql_for_nested_data_explode(self):
        print("Testing HdfsImport readFromSource")
        self.sparksettings = SparkSettings("XmlMapperTest")
        self.spark = self.sparksettings.getSparkSession()
        self.hdfsImport = HdfsImport(self.spark)

        # Read JSON file from given path
        json_df = self.spark.read.json(path='resources/clinical_trial/*.xml')

        json_df.printSchema()

        # Register as temporary view / table for flattening queries to execute on
        json_df.createOrReplaceTempView('jsontable')

        # self.spark.range(10).select(monotonically_increasing_id()).show()
        # self.spark.range(10).select(monotonically_increasing_id()).coalesce(1).show()
        # self.spark.range(10).repartition(5).select(monotonically_increasing_id()).coalesce(1).show()

        # Create an object of class XmlMapper from Mapper.py by passing spark variable
        xml_mapper: ComplexDataMapper = ComplexDataMapper(sc=self.spark)

        # Call createViews function by passing json_df dataframe, it returns 2 things flattening queries and XPATH (
        # Only for XML; Ignore for JSON)
        view_queries = xml_mapper.createViews(df=json_df, root_table_name='jsontable',
                                              columns_cascade_to_leaf_level_with_alias=[
                                                  'item.organizationId AS pk_organizationId'])

        # Loop through all queries, execute them, physicalize flattened attributes as table - Repeat steps to all
        # queries (Nested attributes)
        for q in view_queries[0]:
            print(f'{q}:' f'{view_queries[0][q]}')
            temp_df = self.spark.sql(view_queries[0][q])
            temp_df.rdd.zipWithUniqueId().toDF().printSchema()
            temp_df.createOrReplaceTempView(q)
            select_cols = []
            for col in temp_df.schema.fields:
                if not str(col.dataType).lower().startswith("struct") and not str(col.dataType).lower().startswith(
                        "array"):
                    select_cols.append(col.name)
            print(f"Total partitions = {temp_df.rdd.getNumPartitions()}")
            temp_df.select(select_cols).show()


if __name__ == '__main__':
    unittest.main()
