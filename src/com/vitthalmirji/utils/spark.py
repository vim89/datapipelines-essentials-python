import logging
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import concat_ws, col, floor, rand
from pyspark.sql.types import StringType

from com.vitthalmirji.utils.helpers import log_exception_details, is_null_or_empty


def get_or_create_spark_session(need_hive_support: bool = False,
                                spark_conf: List[dict] = [{'key': 'spark.app.name', 'value': ''}]) -> SparkSession:
    """
    Creates a spark session with given configuration in parameters

    Args:
        :param application_name: Name of the spark application
        :param spark_conf: Specific Spark Configurations at user level (default is None)
        :param need_hive_support: Enable Hive support in spark session? (default is False)

    Returns:
        An object of SparkSession

    Exceptions:
        Throws any exception on to calling function that has encountered during creating SparkSession
        :exception type of exception is broader, this can be improvised to handle more specific exceptions
    """
    spark: SparkSession
    try:
        spark: SparkSession = SparkSession.getActiveSession()
        if spark:
            logging.warning("Returning active spark session")
            return spark

        logging.warning(f"Creating spark session first time with configs {spark_conf}")

        if need_hive_support:
            spark = SparkSession.builder \
                .enableHiveSupport() \
                .getOrCreate()
        else:
            spark = SparkSession.builder \
                .getOrCreate()

        for conf in list(spark_conf):
            spark.conf.set(**conf)

        logging.warning(f"Executor cores = {spark.conf.get('spark.executor.cores', 'Not set')}")
        logging.warning(f"Num Executors = {spark.conf.get('spark.executor.instances', 'Not set')}")
        return spark
    except Exception as ex:
        log_exception_details(message="Error creating spark session", exception_object=ex)
        raise ex


def read_data_as_spark_dataframe(filetype: str, location: str, options={}, table_name=None) -> DataFrame:
    """
    Reads various kind of files & tables in spark
    Args:
        :param filetype:
        :param location:
        :param options:
        :param table_name:

    Returns:
        :return: A DataFrame object

    Exception:
        Throws any exception that is encountered during file / table read in spark
        :exception type of exception is broader, this can be improvised to handle more specific exceptions
    """
    logging.warning(f"Attempting to read {filetype} in spark using configs {options} from location {location}")
    spark = get_or_create_spark_session()
    try:
        if str(filetype).lower().__eq__('table'):
            if is_null_or_empty(table_name) is not None:
                try:
                    _ = spark.read.options(**options).table(table_name)
                except Exception as ex:
                    log_exception_details(message=f"Error reading table {table_name}", exception_object=ex)
                    raise ex
            else:
                print(f"Invalid table {table_name} -Table do not exist in SQL Context: ")
        elif str(filetype).lower().__eq__('text'):
            logging.warning(
                "Lines will be read from the text file and dataframe will have single column by name 'line'")
            return spark.read.options(**options).text(paths=location).toDF('line')
        elif str(filetype).lower().__eq__('csv'):
            return spark.read.options(**options).csv(path=location)
        elif str(filetype).lower().__eq__('xml'):
            return spark.read.format('com.databricks.spark.xml').options(**options).load(path=location)
        elif str(filetype).lower().__eq__('json'):
            return spark.read.options(**options).json(path=location)
        elif str(filetype).lower().__eq__('orc'):
            return spark.read.options(**options).orc(location)
        elif str(filetype).lower().__eq__('parquet'):
            return spark.read.options(**options).parquet(location)
        else:
            raise Exception(f"Invalid filetype: {filetype}")
    except Exception as ex:
        log_exception_details(message=f"Error reading file in Spark of filetype {filetype}", exception_object=ex)
        raise ex


def revise_shuffle_partitions(multiplier: int = 1):
    """
    Sets the shuffle partition to total number of cores across all executors
    Useful in dataframe operations using spark
    :param multiplier: In case of stage failures increase the multiplier
    :return: N/A
    """
    spark = get_or_create_spark_session()
    num_executors = int(spark.conf.get('spark.executor.instances', '2').strip())
    num_cores = int(spark.conf.get('spark.executors.cores', '1').strip())
    revised_shuffle_partition = num_executors * num_cores * multiplier
    spark.conf.set('spark.sql.shuffle.partitions', f"{revised_shuffle_partition}")


def data_frame_repartition(df: DataFrame, num_files: int = None, use_coalesce=False, repartition_columns=None):
    """
    Function to repartition data for better performance.
    Majorly has 2 types: #1 - coalesce: to narrow down files in output; #2 - repartition: to uniformly distribute data in output
    Note: This involves shuffling (wide transformation)
    Args:
        :param df: Dataframe on which repartition (wide transformation) to be performed
        :param num_files: Number of output files required
        :param use_coalesce: Use this to narrow down the number of files irrespective of any columns default is False
        :param repartition_columns: Columns on which repartition to be performed
                                    Most important note: Columns specified here must & should be low cardinality values in table
    Returns:
        :return: Dataframe with repartition or coalesce transformation applied
    """
    if use_coalesce:
        return df.coalesce(num_files)

    columns_list = list(map(lambda column: col(column).cast(StringType()),
                            repartition_columns)) if repartition_columns is not None else []

    if num_files is None and len(columns_list) > 0:
        return df.repartition(*columns_list)

    salting_column = floor(rand() * num_files)
    temp_repartition_column = 'temp_repartition_column'
    return df.withColumn(
        temp_repartition_column,
        concat_ws('~', *columns_list, salting_column)
    ).repartition(temp_repartition_column).drop(temp_repartition_column)


def standardize_and_rename_df_columns(df: DataFrame, column_names_to_rename: dict):
    """
    Performs renaming column names on given dataframe:
    Trims if column name has leading & trailing whitespaces
    For given dictionary of columns renames according to specified name
    Args:
        :param df: DataFrame for renaming columns
        :param column_names_to_rename: dictionary having existing column name & revised / renaming column name

    Returns:
        :return: _df transformed dataframe with column names renamed

    Exceptions:
        :exception Throws exception that's encountered during renaming column on dataframe
    """
    _df = df
    try:
        # Trim and lowercase all column names
        for column_name in filter(lambda c: not column_names_to_rename.keys().__contains__(c), df.columns):
            _df = _df.withColumnRenamed(column_name, column_name.strip().lower())

        for column_name, revised_column_name in column_names_to_rename.items():
            _df = _df.withColumnRenamed(column_name, revised_column_name)
        return _df
    except Exception as ex:
        log_exception_details(message=f"Error renaming columns on given dataframe {column_names_to_rename}",
                              exception_object=ex)
        raise ex
