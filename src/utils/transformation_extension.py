from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame


def transform(self, f):
    return f(self)


RDD.transform = transform
DataFrame.transform = transform
