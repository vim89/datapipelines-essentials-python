import unittest

from pyspark.sql import SparkSession

from com.hellofresh.utils.comprehensive_logging import init_logging
from com.hellofresh.utils.spark import get_or_create_spark_session, standardize_and_rename_df_columns, \
    read_data_as_spark_dataframe, data_frame_repartition


class UtilsSparkTestCases(unittest.TestCase):
    init_logging(job_name='UtilsSparkTestCases')

    def test_create_spark_session(self):
        spark: SparkSession = get_or_create_spark_session()
        self.assertIsNot(spark, None)
        self.assertEqual(spark, SparkSession.getActiveSession())
        self.assertEqual(spark.sparkContext.appName.__str__(), 'pyspark-shell')

    def test_standardize_and_rename_df_columns(self):
        spark = get_or_create_spark_session()
        data = [('Category A', 100, "This is category A"),
                ('Category B', 120, "This is category B"),
                ('Category C', 150, "This is category C")]
        df = spark.sparkContext.parallelize(data).toDF(['cateGory ', ' iD ', 'category description'])

        self.assertEqual(df.columns, ['cateGory ', ' iD ', 'category description'])

        df = standardize_and_rename_df_columns(df=df,
                                               column_names_to_rename={'category description': 'category_description'})
        self.assertEqual(df.columns, ['category', 'id', 'category_description'])

    def test_negative_cases_for_read_data_as_spark_dataframe(self):

        # INVALID
        try:
            df = read_data_as_spark_dataframe(filetype='invalid', location='a://a.txt')
        except Exception as ex:
            print(ex.__str__())
            self.assertEqual(ex.__str__(), 'Invalid filetype: invalid')

        # CSV
        try:
            df = read_data_as_spark_dataframe(filetype='csv', location='a://a.csv')
            csv_read = 'successful'
        except Exception as ex:
            csv_read = 'failed'

        self.assertAlmostEqual(csv_read, 'failed')

        # TEXT
        try:
            df = read_data_as_spark_dataframe(filetype='text', location='a://a.txt')
            text_read = 'successful'
        except Exception as ex:
            text_read = 'failed'

        self.assertAlmostEqual(text_read, 'failed')

        # XML
        try:
            df = read_data_as_spark_dataframe(filetype='xml', location='a://a.xml')
            xml_read = 'successful'
        except Exception as ex:
            xml_read = 'failed'

        self.assertAlmostEqual(xml_read, 'failed')

        # Table
        try:
            df = read_data_as_spark_dataframe(filetype='table', location='a://a.xml')
            table_read = 'successful'
        except Exception as ex:
            table_read = 'failed'

        self.assertAlmostEqual(table_read, 'failed')

    def test_data_frame_repartition(self):
        spark = get_or_create_spark_session()
        data = [('Category A', 100, "This is category A"),
                ('Category B', 120, "This is category B"),
                ('Category C', 150, "This is category C")]
        df = spark.sparkContext.parallelize(data).toDF(['category', 'id', 'category_description'])

        df = data_frame_repartition(df=df, use_coalesce=True, num_files=1)
        self.assertTrue(df is not None)

        df = data_frame_repartition(df=df, num_files=5, repartition_columns=['category'])
        self.assertFalse(df.columns.__contains__('temp_repartition_column'))


if __name__ == '__main__':
    unittest.main()
