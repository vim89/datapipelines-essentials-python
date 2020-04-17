import json
import os
import subprocess
import zipfile

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame

from datawarehousing.change_data_capture import append_audit_attributes_to_xml, add_audit_columns, identify_new_records, \
    add_row_number_to_dataframe
from mapper.Mapper import ComplexDataMapper
from utils.Utilities import create_spark_session, create_local_aws_s3_platform, delete_s3_bucket, create_s3_bucket, \
    is_url_valid, delete_s3_directory, create_s3_directory, download_file_from_web, get_file_names_in_zip, upload_to_s3, \
    list_s3_files, read_files_as_spark_dataframe


def download_files_to_s3(download_file_url, broadcast_parameters):
    parameters = broadcast_parameters.value
    url_suffix = str(download_file_url).split('/')[-1].replace('=', '_').replace('?', '_').replace('.', '_').replace(
        '-', '_')
    tgt_file_name = f"{url_suffix}_{parameters['download_target_filename']}"
    local_download_path = download_file_from_web(download_file_url, tgt_file_name=f"/tmp/{tgt_file_name}")

    files_in_zip = get_file_names_in_zip(zip_file=local_download_path, filter_pattern='.zip',
                                         filter_condition='ends_with')

    zip_file_obj: zipfile.ZipFile(local_download_path, "r")
    zip_file_name = str(str(local_download_path).split('/')[-1]).split('.')[0]

    one_big_xml_file = f"/tmp/{zip_file_name}.xml"

    with open(one_big_xml_file, "w+", encoding='utf-8') as one_big_xml_file_obj:
        for file in files_in_zip:
            print(f"Writing contents of {file} to file {one_big_xml_file}")
            updated_file_content = append_audit_attributes_to_xml(file, zip_file_obj.open(file).read(),
                                                                  parameters['xml_closing_tag'])
            # print(updated_file_content)
            one_big_xml_file_obj.write(updated_file_content)

    upload_to_s3(file_name=one_big_xml_file, bucket=parameters['bucket'],
                 prefix=f"{parameters['=landing_directory']}/{zip_file_name}/")
    one_big_xml_file_obj.close()
    os.remove(one_big_xml_file)

    os.remove(local_download_path)


def write_dataframe_to_postgres(spark: SparkSession, _df: DataFrame, opt, primary_keys):
    total_executors = int(spark.conf.get("spark.executor.instances", default="4").strip())
    total_cores = int(spark.conf.get("spark.executor.cores", default="1").strip())
    total_paritions_revised = total_cores * total_executors

    print(f"Total Executors = {total_executors}")
    print(f"Total Cores = {total_cores}")
    print(f"Total revised paritions = {total_paritions_revised}")

    df: DataFrame = _df
    df.repartition(total_paritions_revised, *primary_keys).write.partitionBy(*primary_keys).format("jdbc") \
        .mode('overwrite') \
        .options(**opt) \
        .save()


def main():
    try:
        job_name = 'clinical_trial_etl'
        spark: SparkSession = create_spark_session(application_name=job_name, need_hive_support=True)

        s3_resource_obj, s3_client_obj = create_local_aws_s3_platform(spark)

        # start moto server, by default it runs on localhost on port 5000
        # Disable this if you run on Hadoop / AWS Glue / AWS Spark / AWS EMR
        process = subprocess.Popen(
            ['moto_server', 's3'],
            stdout=subprocess.PIPE,
            shell=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )

        with open('resources/job_parameters/clinical_trial.json', 'r') as param_file:
            parameters = json.load(param_file.read())[job_name]
        param_file.close()

        broadcast_parameters = spark.sparkContext.broadcast(parameters)

        print(f"Parameters Dictionary = {parameters}")

        # Delete bucket during initial / full load otherwise comment the below lines
        delete_s3_bucket(param_file['bucket'])

        # Create bucket
        create_s3_bucket(param_file['bucket'])

        # Download files from https://clinicaltrials.gov/
        url_prefix = parameters['download_url_prefix']
        max_chunk = parameters[
            'max_chunk_range']  # There are 40+ chunks in available from official https://clinicaltrials.gov/

        print(f"Downloading files from {parameters['download_url_prefix']}")

        def return_valid_url(u):
            ret_code, is_valid = is_url_valid(url=u)
            if is_valid:
                return u
            else:
                return None

        valid_url_list = list(filter(lambda u: u is not None, list(
            map(lambda n: return_valid_url(f"{url_prefix}{n}"), list(range(1, max_chunk))))))
        print(f"Valid URL list = {valid_url_list}")

        url_rdd: RDD = spark.sparkContext.parallelize(valid_url_list)

        # Delete landing directory prepare to ingest data
        delete_s3_directory(bucket_name=parameters['bucket'], directory_name=parameters['landing_directory'])

        # Create landing, staging & audit directories
        create_s3_directory(bucket_name=parameters['bucket'], directory_name=parameters['landing_directory'])
        create_s3_directory(bucket_name=parameters['bucket'], directory_name=parameters['staging_directory'])
        create_s3_directory(bucket_name=parameters['bucket'], directory_name=parameters['audit_directory'])

        # Download zip files, unzip & collect all XML into one big XML file & write XML files to AWS S3
        url_rdd.foreach(lambda uri: download_files_to_s3(download_file_url=uri, parameters=broadcast_parameters))

        print(list_s3_files(opt={'Bucket': parameters['bucket'], 'Prefix': parameters['landing_directory']}))

        # Upload default xml file to S3 Raw
        upload_to_s3(file_name='resources/xml/default_clinical_study.xml', bucket=parameters['bucket'],
                     prefix=f"{parameters['landing_directory']}/default/")

        # Read XML and Staging data as preparation to CDC Operation
        try:
            xml_file_read_options = {
                'rootTag': 'clinical_study',
                'rowTag': 'clinical_study',
                'attributeValue': 'xmlvaluetag',
                'valueTag': 'xmltagvalue',
                'attributePrefix': 'xmlattribute_'
            }
            new_df: DataFrame = read_files_as_spark_dataframe(spark=spark,
                                                              location=f"s3a://{parameters['bucket']}/{[parameters]['clinical_trail']['landing_directory']}/*/*.xml",
                                                              filetype='xml', opt=xml_file_read_options)
        except Exception as ex:
            import traceback
            print(f"No new XML files found {traceback.format_exc()}")
            exit(0)
        new_df: DataFrame = add_audit_columns(_df=new_df)
        # new_df.printSchema()
        print(f"Total new XML records = {new_df.count()}")

        try:
            old_df: DataFrame = read_files_as_spark_dataframe(spark=spark,
                                                              location=f"s3a://{parameters['bucket']}/{parameters['staging_directory']}/",
                                                              filetype='json')
            old_df = old_df
        except Exception as ex:
            import traceback
            print(f"Error reading staging {traceback.format_exc()}")
            old_df: DataFrame = None

        # CDC Operation
        final_df: DataFrame = identify_new_records(spark=spark, old_dataframe=old_df,
                                                   new_dataframe=new_df,
                                                   primary_keys=parameters['primary_keys'],
                                                   order_by_keys=parameters['order_by_keys'])

        new_records_count = final_df.count()
        print(f"CDC Changes = {new_records_count}")
        # final_df.select('id_info.nct_id', 'xml_file_name', 'hashcode', 'spark_timestamp').orderBy(
        # 'spark_timestamp', ascending=False).show()

        if new_records_count > 0:
            final_df.write.mode('append').json(
                f"s3a://{parameters['raw_bucket']}/{parameters['staging_directory']}/")

        staging_df: DataFrame = spark.read.json(
            f"s3a://{parameters['raw_bucket']}/{parameters['staging_directory']}/")

        staging_df.select('id_info.nct_id', 'xml_file_name', 'hashcode', 'spark_timestamp').orderBy('spark_timestamp',
                                                                                                    ascending=False).show()
        print(f"Total in staging after CDC = {staging_df.count()}")
        # staging_df.printSchema()

        # Snapshot
        snapshot_df: DataFrame = add_row_number_to_dataframe(dataframe=staging_df,
                                                             primary_keys=parameters['primary_keys'],
                                                             order_by_keys=parameters['order_by_keys'],
                                                             eliminate_duplicate_records=True,
                                                             drop_row_number_column=True)

        print(f"Total in active records after CDC = {snapshot_df.count()}")

        # Explode XML into Subset tables
        complex_data_mapper: ComplexDataMapper = ComplexDataMapper(sc=spark)

        views, xpath = complex_data_mapper.createViews(snapshot_df, root_table_name='xmltable',
                                                       columns_cascade_to_leaf_level_with_alias=parameters[
                                                           'primary_keys'])

        snapshot_df.createOrReplaceTempView('xmltable')
        for k, v in views.items():
            print(f"key = {k}; value = {v}")
            tmp_df: DataFrame = spark.sql(v)
            tmp_df.createOrReplaceTempView(k)
            # tmp_df.show()

        # Write to Aurora / Postgres
        # transform_query = config_archive.open('resources/sql/clinical_trail_sponsors.sql').read().decode('utf-8')
        sql_transformation_files = list(map(lambda root_dir, _dir, file: file,
                                            filter(lambda root_dir, _dir, file: str(file).endswith('.sql'),
                                                   os.walk('resources/sql/transformations'))))
        db_options = {'url': f'jdbc:postgresql://localhost:5432/postgres?user=postgres&password=password',
                      'truncate': 'true'}
        schema = 'ingest'
        for sql_file in sql_transformation_files:
            tbl_name = str(sql_file).split('/')[-1].split('.')[0]
            db_options.update({'dbtable': f'{schema}.{tbl_name}'})
            transform_query = open(sql_file, 'r').read()
            transform_query = transform_query.replace('{prcs_run_id} AS prcs_run_id,', '100 AS prcs_run_id,') \
                .replace('{job_run_id} AS job_run_id,', '101 AS job_run_id,') \
                .replace('{rec_crt_user} AS rec_crt_user', f"'{spark.sparkContext.sparkUser()}' AS rec_crt_user")
            print(f"Loading table {tbl_name}")
            print(f"Query = {transform_query}")
            try:
                write_df = spark.sql(transform_query)
                write_dataframe_to_postgres(spark=spark, _df=write_df, opt=db_options,
                                            primary_keys=parameters['target_primary_keys'])
            except:
                import traceback
                print(f"Failed for table {tbl_name}")
                print(f"Root cause is {traceback.format_exc()}")
    except Exception as ex:
        import traceback
        print(f"Error clinical_trial_etl job failed - {traceback.format_exc()}")
        exit(1)


if __name__ == '__main__':
    main()
