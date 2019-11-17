# Datalake ETL Pipeline
Data transformation simplified for any Data platform.

`Features:` The package has complete ETL process - 
1. Uses metadata, transformation & data model information to design ETL pipeline
2. Builds target transformation SparkSQL and Spark Dataframes
3. Builds source & target Hive DDLs
4. Validates DataFrames, extends core classes, defines DataFrame transformations, and provides UDF SQL functions.
5. Supports below fundamental transformations for ETL pipeline -
   * Filters on source & target dataframes
   * Grouping and Aggregations on source & target dataframes
   * Heavily nested queries / dataframes
6. Has complex and heavily nested XML, JSON, Parquet & ORC parser to nth
   level of nesting
7. Has Unit test cases designed on function/method level & measures
  source code coverage
8. Has information about delpoying to higher environments
9. Has API documentation for customization & enhancement

`Enhancements:` In progress -
1. Integrate Audit and logging - Define Error codes, log process
   failures, Audit progress & runtime information