import argparse
import datetime
import logging
from functools import reduce
from pathlib import Path

import isodate
from pyspark.sql.functions import col, trim, udf, collect_list, coalesce, lit, when, split
from pyspark.sql.types import StructType, StringType, StructField, DateType, IntegerType

from com.vitthalmirji.utils.comprehensive_logging import init_logging
from com.vitthalmirji.utils.data_quality import DataQuality
from com.vitthalmirji.utils.helpers import read_json_get_dict, log_exception_details, convert_iso_to_time_duration, \
    add_iso_time_duration
from com.vitthalmirji.utils.spark import get_or_create_spark_session, revise_shuffle_partitions, \
    read_data_as_spark_dataframe, standardize_and_rename_df_columns, data_frame_repartition


def main(args, task1_dq_rules_conf_file, task2_dq_rules_conf_file):
    """
    :param args:
    :param task1_dq_rules_conf_file:
    :param task2_dq_rules_conf_file:
    :return:
    """
    init_logging(job_name='HelloFresh Recipes Data Engineering')
    logging.info(f"HelloFresh Recipes Data Engineering Test - main function")
    logging.warning(f'Commandline args = {args}')

    spark = get_or_create_spark_session(
        spark_conf=[{'key': 'spark.app.name', 'value': 'vitthalmirji-data-engg-recipe-tasks'}])

    revise_shuffle_partitions()

    task1_dq_rules = DataQuality(
        **read_json_get_dict(json_path=task1_dq_rules_conf_file)) if Path(task1_dq_rules_conf_file).is_file() else None

    # Task 1 - read the source data, pre-process it and persist (write) it to ensure optimal structure and performance
    # for further processing - more details in function doc
    task1(input_data_path=args['input_data_dir'], output_data_path=f"{args['output_data_dir']}/task1",
          input_file_type='json',
          spark_opts={'encoding': 'utf-8'}, dq_rules=task1_dq_rules)

    task2_dq_rules = DataQuality(
        **read_json_get_dict(json_path=task2_dq_rules_conf_file)) if Path(task2_dq_rules_conf_file).is_file() else None

    # Task 2 - Transformations, Calculation: Aggregation of total_cooking_time and
    # average_total_cooking_time per difficulty - more details in function doc
    task2(input_data_path=f"{args['output_data_dir']}/task1", output_data_path=f"{args['output_data_dir']}/task2",
          input_file_type='parquet', dq_rules=task2_dq_rules)


def task1(input_data_path: str, output_data_path: str, input_file_type: str, spark_opts: dict,
          dq_rules: DataQuality = None):
    """
    Using Apache Spark and Python (pyspark), read the source data, pre-process it and persist (write) it,
    to ensure optimal structure and performance for further processing.
    The source events are located on the `input` folder.

    Args:
        :param input_data_path: Path of data to read as input
        :param output_data_path: Path for writing ouput data
        :param input_file_type: Type of input data file (json, csv, xml, table etc.)
        :param spark_opts: Options to consider while read data - Example: encoding must be utf-8
        :param dq_rules: Data quality rules to check & report on input data

    Returns:
        :return: N/A

    Exceptions:
        :exception Throws exception on to calling function if anything goes wrong in performing task1
    """
    try:
        # Read data from source using function that handles various file type reads using custom options
        df = read_data_as_spark_dataframe(filetype=input_file_type, options=spark_opts, location=input_data_path)

        # Execute data quality rules if valid DataQuality object of rules is passed in parameter
        if dq_rules:
            execution_result = dq_rules.execute_rules(df=df)
            if not execution_result[0]:
                logging.error(
                    f"Data quality rules failed, check execution details report here: {dq_rules.execution_reports_dir}")
            logging.info("Data quality rules execution successful")
            dq_rules.write_report_to_html(file_name="task1-dq-report.html")

        # Pre-processing transformations..

        # Rename column names to lower case and underscore separated to follow best practices of preprocessing
        df = standardize_and_rename_df_columns(df=df, column_names_to_rename={'cookTime': 'cook_time',
                                                                              'datePublished': 'date_published',
                                                                              'prepTime': 'prep_time',
                                                                              'recipeYield': 'recipe_yield'})
        # df.printSchema()

        # Either we can ignore NULL / Empty values in prep_time & cook_time columns OR
        # We can assume 0 hours / minutes to prepare & cook that particular recipe
        # Here I am assuming 0 hours / minutes to prepare & cook that recipe
        cook_time_default_col_value = lit("PT")
        prep_time_default_col_value = lit("PT")
        df = df.select(
            when(col('cook_time').isNull(), cook_time_default_col_value).when(col('cook_time').isin(''),
                                                                              cook_time_default_col_value).otherwise(
                trim(col('cook_time'))).alias('cook_time'),
            when(col('prep_time').isNull(), prep_time_default_col_value).when(col('prep_time').isin(''),
                                                                              prep_time_default_col_value).otherwise(
                trim(col('prep_time'))).alias('prep_time'),
            trim(col('date_published')).cast(DateType()).alias('date_published'),
            trim(col('name')).alias('name'),
            trim(col('recipe_yield')).cast(IntegerType()).alias('recipe_yield'),
            split(col('ingredients'), "\n").alias('ingredients'),
            trim(col('description')).alias('description'),
            trim(col('url')).alias('url'),
            trim(col('image')).alias('image')
        )

        # df.printSchema()
        # df.show(truncate=False)
        df = data_frame_repartition(df=df, repartition_columns=['date_published'])
        df.write.mode('overwrite').parquet(output_data_path)
    except Exception as ex:
        log_exception_details(message="Error executing task1, check logs", exception_object=ex)
        raise ex


def task2(input_data_path: str, input_file_type: str, output_data_path: str, dq_rules: DataQuality = None):
    """
    Task 2 -
    Using Apache Spark and Python read the processed dataset from Task 1 and:
        1. Extract only recipes that have beef as one of the ingredients.
        2. Calculate average cooking time duration per difficulty level.
        3. Persist dataset as CSV to the `output` folder.
           The dataset should have 2 columns: difficulty,avg_total_cooking_time.

    Total cooking time duration can be calculated by formula:
        total_cook_time = cookTime + prepTime
        Hint: times represent durations in ISO format.

    Criteria for levels based on total cook time duration:
        1. easy - less than 30 mins
        2. medium - between 30 and 60 mins
        3. hard - more than 60 mins.

    Args:
        :param output_data_path:
        :param input_file_type:
        :param input_data_path:
        :param dq_rules:

    Returns:
        :return: N/A

    Exceptions:
        :exception
    """
    df = read_data_as_spark_dataframe(filetype=input_file_type, location=input_data_path)

    udf_determine_recipe_difficulty = udf(
        lambda time1, time2: determine_cooking_difficulty(cook_time=time1, prep_time=time2),
        StructType([StructField(name='total_time', dataType=StringType()),
                    StructField(name='difficulty', dataType=StringType())]))

    udf_calculate_average_time_per_difficulty = udf(
        lambda time_duration_list: calculate_time_duration_average(
            list(map(lambda t: convert_iso_to_time_duration(t), time_duration_list))),
        StringType())

    output_df = df.withColumn('total_time_difficulty',
                              udf_determine_recipe_difficulty(
                                  when(coalesce(col('cook_time'), lit("PT")).isin(''), lit("PT")).otherwise(
                                      trim(col('cook_time'))),
                                  when(coalesce(col('prep_time'), lit("PT")).isin(''), lit("PT")).otherwise(
                                      trim(col('prep_time'))))
                              ) \
        .groupby(col('total_time_difficulty.difficulty')) \
        .agg(udf_calculate_average_time_per_difficulty(collect_list(col('total_time_difficulty.total_time'))).alias(
        'avg_total_cooking_time'))

    # Execute data quality rules if valid DataQuality object of rules is passed in parameter
    if dq_rules:
        execution_result = dq_rules.execute_rules(df=output_df)
        if not execution_result[0]:
            logging.error(
                f"Data quality rules failed, check execution details report here: {dq_rules.execution_reports_dir}")
        logging.info("Data quality rules execution successful")
        dq_rules.write_report_to_html(file_name="task2-dq-report.html")

    output_df.write.mode('overwrite').options(**{'header': 'true', 'encoding': 'utf-8'}).csv(path=output_data_path)


def calculate_time_duration_average(list_of_total_cooking_time_duration):
    """
    Computes average time duration given

    Args:
        :param list_of_total_cooking_time_duration: list of time duration
    Return:
        :return: Average time in ISO format. Example PT2H43M37.105263S

    Exceptions:
        Thrown by calling functions called in this function
    """
    return str(isodate.duration_isoformat(
        reduce(lambda a, b: a + b, list_of_total_cooking_time_duration) / len(list_of_total_cooking_time_duration)))


def determine_cooking_difficulty(cook_time: str, prep_time: str):
    """
    Determines cooking difficulty as per requirement below.

    total cook time duration = prep_time + cook_time
    Criteria for level of difficulty based on total cook time duration:
        1. easy - less than 30 mins
        2. medium - between 30 and 60 mins
        3. hard - more than 60 mins.
    Args:
        :param cook_time: Cooking time from dataset
        :param prep_time: Preparation time from dataset

    Returns:
        :return: difficulty based on criteria given

    Exceptions:
        Thrown by calling functions called in this function
    """
    total_time_iso = add_iso_time_duration(time1=cook_time, time2=prep_time)
    _total_time = isodate.parse_duration(total_time_iso)
    if _total_time is None:
        difficulty = 'invalid'
    elif _total_time < datetime.timedelta(minutes=30):
        difficulty = 'easy'
    elif _total_time.__eq__(datetime.timedelta(hours=1)) or (
            datetime.timedelta(minutes=30) <= _total_time <= datetime.timedelta(minutes=60)):
        difficulty = 'medium'
    elif _total_time > datetime.timedelta(hours=1) or _total_time > datetime.timedelta(minutes=60):
        difficulty = 'hard'
    else:
        difficulty = 'invalid'

    return str(total_time_iso), difficulty


def parse_command_line_args():
    """
    Parse commandline arguments with given rules below
    Args: N/A

    Returns:
        :return: dict[str, Any] Dictionary of arguments as variables in key,value pair

    Exceptions:
        Thrown by calling functions called in this function
    """
    parser = argparse.ArgumentParser(description='HelloFresh Data Engineering Recipe tasks from '
                                                 'https://github.com/vim89')
    parser.add_argument('-i', '--input-data-dir', dest='input_data_dir', required=True,
                        help='Input data directory')
    parser.add_argument('-o', '--output-data-dir', dest='output_data_dir', required=True,
                        help='Directory for writing output data', )
    args = parser.parse_args().__dict__
    return dict(args)


if __name__ == '__main__':
    command_line_args = parse_command_line_args()
    print(command_line_args)
    main(args=command_line_args, task1_dq_rules_conf_file="recipe-task1-dq-rules.json",
         task2_dq_rules_conf_file="recipe-task2-dq-rules.json")
