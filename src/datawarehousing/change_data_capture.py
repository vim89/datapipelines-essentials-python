import hashlib

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, row_number

from utils.Utilities import is_null_or_empty


def append_audit_attributes_to_xml(file, file_contents, xml_closing_tag):
    hash_val = hashlib.md5(file_contents.encode('utf-8')).hexdigest()
    return str(file_contents).replace(f'</{xml_closing_tag}>',
                                      f'<hashcode>{hash_val}</hashcode'
                                      f'><xml_file_name>'
                                      f'{str(file)}</xml_file_name></'
                                      f'{xml_closing_tag}>')


def add_row_number_to_dataframe(dataframe: DataFrame, primary_keys, order_by_keys, eliminate_duplicate_records=False,
                                drop_row_number_column=False):
    window = Window.partitionBy(
        *list(map(lambda c: col(c), primary_keys))).orderBy(
        *list(map(lambda c: col(c).desc(), order_by_keys)))
    row_num_col = row_number().over(window=window).alias('row_num')

    if eliminate_duplicate_records and drop_row_number_column:
        return dataframe.withColumn(colName='row_num', col=row_num_col).filter('row_num = 1').drop('row_num')
    elif eliminate_duplicate_records:
        return dataframe.withColumn(colName='row_num', col=row_num_col).filter('row_num = 1')
    else:
        return dataframe.withColumn(colName='row_num', col=row_num_col)


def add_audit_columns(_df: DataFrame) -> DataFrame:
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df: DataFrame = _df
    sel_cols = list(map(lambda x: str(f'`{x}`'), df.schema.names))
    sel_cols.append(f"reverse(split(input_file_name(), '/'))[0] AS spark_file_name")
    sel_cols.append(f"CAST('{ts}' AS TIMESTAMP) AS spark_timestamp")
    print(sel_cols)
    df: DataFrame = df.selectExpr(sel_cols)
    return df


def identify_new_records(spark: SparkSession, old_dataframe: DataFrame, new_dataframe: DataFrame,
                         primary_keys=[], order_by_keys=['current_timestamp']) -> DataFrame:
    old_df = "old_df"
    new_df = "new_df"

    if is_null_or_empty(primary_keys):
        print("WARNING - Empty primary keys given: Assuming all fields in the table for Deduplication")
        dedup_query = f"SELECT *FROM (SELECT t1.*,  row_number() over (order by {','.join(order_by_keys)} desc) as row_num FROM {old_df} t1) WHERE row_num = 1"
    elif is_null_or_empty(old_dataframe) and is_null_or_empty(
            new_dataframe) and new_dataframe.count() <= 0 and old_dataframe.count() <= 0:
        print("Empty Dataframes")
        return None
    elif not is_null_or_empty(new_dataframe) and new_dataframe.count() > 0 and (
            is_null_or_empty(old_dataframe) or old_dataframe.count() <= 0):
        print("Assuming initial load CDC not required")
        return new_dataframe
    else:
        print(f"Before CDC Staging count = {old_dataframe.count()}")
        dedup_query = f"SELECT *FROM (SELECT t1.*, row_number() over (partition by {','.join(primary_keys)} order by {','.join(order_by_keys)} desc) as row_num FROM {old_df} t1) WHERE row_num = 1"
        old_dataframe.createOrReplaceTempView(old_df)
        new_dataframe.createOrReplaceTempView(new_df)
        spark.sql(dedup_query).createOrReplaceTempView(old_df)

        join_condition = list(map(lambda x: str(f'{old_df}.{x} = {new_df}.{x}'), primary_keys))
        exclude_condition = list(map(lambda x: str(f'{old_df}.{x} IS NULL'), primary_keys))
        new_pks_query = f"SELECT {new_df}.* FROM {new_df} LEFT JOIN {old_df} ON {' AND '.join(join_condition)} WHERE {' AND '.join(exclude_condition)}"
        updates_query = f"SELECT {new_df}.* FROM {new_df} INNER JOIN {old_df} ON {' AND '.join(join_condition)} WHERE {new_df}.hashcode <> {old_df}.hashcode"
        print(f"Fetch only New PK records query = {new_pks_query}")
        print(f"Fetch updated records query = {updates_query}")
        new_pk_records_df: DataFrame = spark.sql(new_pks_query).dropDuplicates()
        updates_df: DataFrame = spark.sql(updates_query).dropDuplicates()

        return new_pk_records_df.union(updates_df)
