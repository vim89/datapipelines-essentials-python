import abc
from abc import ABC

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from etl import ETL


class IImport:
    spark: SparkSession
    system: str
    table: str

    @abc.abstractmethod
    def readFromSource(self, location, filetype, opt={}, tbl=""): DataFrame

    @abc.abstractmethod
    def cleanup(self, location): None


class HdfsImport(IImport, ABC):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def readFromSource(self, location, filetype, opt={}, tbl="") -> DataFrame:
        try:
            if str(filetype).lower().__eq__('tbl'):
                if ETL.isNullOrEmpty(tbl) is not None:
                    try:
                        _ = self.spark.read.table(tbl)
                    except Exception as ex:
                        print(f"Error reading table {tbl}")
                else:
                    print(f"Invalid table {tbl} -Table do not exist in SQL Context: ")
            elif str(filetype).lower().__eq__('text'):
                return self.spark.read.text(paths=location, wholetext=True).toDF('line')
            elif str(filetype).lower().__eq__('csv'):
                return self.spark.read.options(header=True, inferSchema=True).csv(path=location)
            elif str(filetype).lower().__eq__('xml'):
                print(opt)
                return self.spark.read.format('com.databricks.spark.xml').options(rowTag='HotelDescriptiveContent',
                                                                                  rootTag='HotelDescriptiveContents',
                                                                                  valueTag='xmlvaluetag',
                                                                                  attributePrefix="@").load(
                    path=location)
            elif str(filetype).lower().__eq__('json'):
                return self.spark.read.options(options=opt).json(path=location)
            elif str(filetype).lower().__eq__('orc'):
                return self.spark.read.options(options=opt).orc(location)
            elif str(filetype).lower().__eq__('parquet'):
                return self.spark.read.options(options=opt).parquet(location)
            else:
                raise "Invalid filetype: " + filetype
        except Exception as ex:
            print("Error reading file in Spark of filetype " + filetype + " Error details: " + str(ex))
