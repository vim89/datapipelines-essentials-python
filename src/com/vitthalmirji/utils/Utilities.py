import argparse
import concurrent
import functools
import http
import io
import json
import logging
import os
import re
import subprocess
import time
import traceback
import urllib
import zipfile
from collections import OrderedDict
from datetime import timedelta, datetime
from multiprocessing import Manager
from pathlib import Path
from typing import Optional, List, Dict, Any

import boto3
import requests
import yaml
from pyspark.sql import *
from tabulate import tabulate
from tqdm import tqdm
import logging.config


def split_words(rdd_data):
    return rdd_data.map(lambda line: re.split('[\\s,.;|\\-]', line)).flatMap(
        lambda word: word if len(word) > 0 else None)


def count_words(rdd_data):
    return rdd_data.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)


def return_valid_url(u):
    ret_code, is_valid = is_url_valid(url=u)
    if is_valid:
        return True
    else:
        return False


def create_local_aws_s3_platform(spark: SparkSession):
    s3_resource_obj = boto3.resource('s3', endpoint_url="http://127.0.0.1:5000")
    s3_client_obj = boto3.client('s3', endpoint_url="http://127.0.0.1:5000")

    os.environ['AWS'] = 'test'
    os.environ['AWS'] = 'test'
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # mock the aws credentials to access s3.
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")

    # we point s3a to our moto server.
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
    # we need to configure hadoop to use s3a.
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return s3_resource_obj, s3_client_obj


# List S3 folders and files
def list_s3_files(opt={}, files_only=False, file_extension=None):
    s3_client_obj = boto3.client('s3',
                                 endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_client_obj = boto3.client('s3'")
    if files_only is True:
        print("Listing Files only")
    objects = s3_client_obj.list_objects(**opt)
    all_paths = list(map(lambda d: str(d['Key']), objects['Contents']))
    files_only_list = list(filter(lambda p: not p.endswith('/'), all_paths))

    if files_only is True:
        if file_extension is not None:
            return list(filter(lambda p: p.endswith(file_extension), files_only_list))
        return files_only_list
    return all_paths


def put_string_to_s3_file(string_content, full_file_path, bucket_name):
    s3_client_obj = boto3.client('s3',
                                 endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_client_obj = boto3.client('s3'")
    s3_client_obj.put_object(Body=string_content, Bucket=bucket_name, Key=full_file_path)


def create_s3_directory(bucket_name, directory_name):
    s3_client_obj = boto3.client('s3',
                                 endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_client_obj = boto3.client('s3'")
    s3_client_obj.put_object(Bucket=bucket_name, Key=f"{directory_name.strip('/')}/")


def delete_s3_directory(bucket_name, directory_name):
    s3_resource_obj = boto3.resource('s3',
                                     endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_resource_obj = boto3.resource('s3'")
    s3_resource_obj.Bucket(bucket_name).objects.filter(
        Prefix=f"{str(directory_name).rstrip('/')}/").delete()


def delete_s3_file(bucket_name, full_file_path, directory_name=None):
    s3_resource_obj = boto3.resource('s3',
                                     endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_resource_obj = boto3.resource('s3'")
    s3_resource_obj.Bucket(bucket_name).objects.filter(Prefix=f"{str(directory_name).rstrip('/')}/").delete()


def create_s3_bucket(bucket_name):
    s3_resource_obj = boto3.resource('s3',
                                     endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_resource_obj = boto3.resource('s3'")
    s3_resource_obj.create_bucket(Bucket=f"{bucket_name.rstrip('/')}")


def delete_s3_bucket(bucket_name):
    s3_resource_obj = boto3.resource('s3',
                                     endpoint_url="http://127.0.0.1:5000")  # For local machines running on # moto3 server
    # s3_resource_obj = boto3.resource('s3'")
    s3_resource_obj.Bucket(bucket_name).objects.all().delete()


def download_file_from_web(download_file_url, tgt_file_name):
    try:
        page = urllib.request.urlretrieve(download_file_url, tgt_file_name)
        print(f"Download {tgt_file_name} complete!")
    except http.client.IncompleteRead as e:
        page = e.partial

    print(page)


def upload_to_s3(file_name, bucket, prefix=None):
    print(f"Uploading file {file_name} to file {bucket}/{prefix}/{file_name.split('/')[-1]}")
    # s3_client_obj = boto3.client('s3')
    s3_client_obj = boto3.client('s3', endpoint_url="http://127.0.0.1:5000")  # For local machines usinf moto3 server
    if prefix is not None and not str(prefix).strip().__eq__(''):
        s3_client_obj.put_object(Bucket=bucket, Key=f"{prefix.rstrip('/')}/")
        s3_client_obj.upload_file(Filename=file_name, Bucket=bucket,
                                  Key=f'{prefix.rstrip("/")}/{file_name.split("/")[-1]}')
    else:
        s3_client_obj.upload_file(Filename=file_name, Bucket=bucket,
                                  Key=file_name.split('/')[-1])


def progress_bar(tgt_file_name, current, total, width=80):
    if current < 0 or total < 0:
        print(
            f"File {tgt_file_name} is downloading but the total size of file is undefined.. please wait until "
            f"download completes..")
    else:
        print(f"Downloading {tgt_file_name}: %d%% [%d / %d] bytes" % (current / total * 100, current, total))


def is_url_valid(url):
    print(f"Checking if {url} is valid..?")
    request = requests.get(url, verify=False)
    return_code = request.status_code
    return return_code, return_code == 200


def is_null_or_empty(obj):
    if obj is None:
        return True
    elif type(obj) is str and str(obj).strip().__eq__(''):
        return True
    else:
        return False


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


# Reading zipped folder data in Pyspark
def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    return file_obj.namelist()


def get_file_names_in_zip(zip_file_path, filter_pattern, filter_condition):
    def apply_filter(f):
        if str(filter_condition).__eq__('ends_with'):
            return str(f).endswith(filter_pattern)
        elif str(filter_condition).__eq__('starts_with'):
            return str(f).startswith(filter_pattern)
        else:
            return str(f).__contains__(filter_pattern)

    zip_file_obj = zipfile.ZipFile(zip_file_path, "r")
    files = list(filter(apply_filter, zip_file_obj.namelist()))
    return files


def zip_extract_read_files(zip_file, filter_pattern, filter_condition):
    print(f"Unzipping {zip_file}")

    def apply_filter(f):
        if str(filter_condition).__eq__('ends_with'):
            return str(f).endswith(filter_pattern)
        elif str(filter_condition).__eq__('starts_with'):
            return str(f).startswith(filter_pattern)
        else:
            return str(f).__contains__(filter_pattern)

    file_obj = zipfile.ZipFile(zip_file, "r")
    files = list(filter(apply_filter, file_obj.namelist()))

    # zip_file_name = str(str(x).split('/')[-1]).split('.')[0]
    # file_names_with_zip_file_name = list(map(lambda z: f"{zip_file_name}/{zip_file_name}_{z}", files))
    # return dict(zip(file_names_with_zip_file_name, list(map(lambda file: file_obj.read(file), files))))
    return dict(zip(files, list(map(lambda file: file_obj.read(file), files))))


# Reading zipped folder data in Pyspark filter by filename
def zip_extract_spark_binary_files_filter(x, filter_pattern, filter_condition):
    def apply_filter(f):
        if str(filter_condition).__eq__('ends_with'):
            return str(f).endswith(filter_pattern)
        elif str(filter_condition).__eq__('starts_with'):
            return str(f).startswith(filter_pattern)
        else:
            return str(f).__contains__(filter_pattern)

    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    return list(filter(apply_filter, file_obj.namelist()))


def download_files_to_s3(download_file_url, target_filename, s3_client_obj, bucket_name):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=str(download_file_url).strip().split('/')[-1]) as t:
        urllib.request.urlretrieve(download_file_url, filename=target_filename, reporthook=t.update_to)
    with open(target_filename, "rb") as temp_file:
        print(f"Step 2 - Uploading {target_filename} from local to S3 {bucket_name} {target_filename}")
        s3_client_obj.upload_fileobj(temp_file, bucket_name, target_filename)


def create_spark_session(application_name, need_hive_support=False,
                         spark_confs=[{'key': 'spark.app.name', 'value': ''}]):
    try:
        if need_hive_support:
            spark = SparkSession.builder \
                .appName(application_name) \
                .enableHiveSupport() \
                .getOrCreate()
        else:
            spark = SparkSession.builder \
                .appName(application_name) \
                .getOrCreate()

        for conf in list(filter(lambda conf: not conf['key'].__eq__('spark.app.name'), spark_confs)):
            spark.conf.set(**conf)

        return spark
    except Exception as ex:
        import traceback
        print(f"Error creating spark session {traceback.format_exc()}")


def read_files_as_spark_dataframe(spark: SparkSession, location, filetype, opt={}, tbl="") -> DataFrame:
    try:
        if str(filetype).lower().__eq__('tbl'):
            if is_null_or_empty(tbl) is not None:
                try:
                    _ = spark.read.options(**opt).table(tbl)
                except Exception as ex:
                    print(f"Error reading table {tbl}")
            else:
                print(f"Invalid table {tbl} -Table do not exist in SQL Context: ")
        elif str(filetype).lower().__eq__('text'):
            return spark.read.options(**opt).text(paths=location, wholetext=True).toDF('line')
        elif str(filetype).lower().__eq__('csv'):
            return spark.read.options(**opt).csv(path=location)
        elif str(filetype).lower().__eq__('xml'):
            print(opt)
            return spark.read.format('com.databricks.spark.xml').options(**opt).load(path=location)
        elif str(filetype).lower().__eq__('json'):
            return spark.read.options(**opt).json(path=location)
        elif str(filetype).lower().__eq__('orc'):
            return spark.read.options(**opt).orc(location)
        elif str(filetype).lower().__eq__('parquet'):
            return spark.read.options(**opt).parquet(location)
        else:
            raise Exception(f"Invalid filetype: {filetype}")
    except Exception as ex:
        import traceback
        print(f"Error reading file in Spark of filetype {filetype} - {traceback.format_exc()}")


class JobContext(object):
    def __init__(self, sc):
        self.counters = OrderedDict()
        self._init_accumulators(sc)
        self._init_shared_data(sc)

    def _init_accumulators(self, sc):
        pass

    def _init_shared_data(self, sc):
        pass

    def initalize_counter(self, sc, name):
        self.counters[name] = sc.accumulator(0)

    def inc_counter(self, name, value=1):
        if name not in self.counters:
            raise ValueError("%s counter was not initialized. (%s)" % (name, self.counters.keys()))

        self.counters[name] += value

    def print_accumulators(self):
        print(tabulate(self.counters.items(), self.counters.keys(), tablefmt="simple"))


START_TIME = datetime.now().isoformat().__str__()


def read_json_get_dict(json_path):
    try:
        with open(json_path, 'r') as stream:
            config = json.load(stream)
        stream.close()
        return config
    except Exception as ex:
        print_exception_details(f'Error reading json file {json_path}, error traceback below')


def read_yaml_get_dict(yaml_path):
    try:
        with open(yaml_path, 'r') as stream:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        stream.close()
        return config
    except Exception as ex:
        print_exception_details(f'Error reading yaml file {yaml_path}, error traceback below')


def parse_arguments(args) -> Dict[str, Any]:
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(f"--{arg['name']}", default=arg['default'])
    cmd_args = parser.parse_args()
    return cmd_args.__dict__


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent.parent


def is_null(o) -> bool:
    if o is None:
        return True
    if str(o).__len__() < 1:
        return True
    return False


def print_exception_details(message, exception_object):
    logging.exception(message)
    traceback.format_exc()


def cast_string_to_date(date_as_string, date_pattern) -> Optional[datetime]:
    try:
        date_time_object = datetime.strptime(str(date_as_string).strip(), date_pattern)
        return date_time_object
    except Exception as ex:
        print_exception_details(message='Error casting date: Check below error traceback', exception_object=ex)
        return None


def get_dates_between_range(refresh_type, start_date, end_date, interval_in_days,
                            date_pattern='%Y-%m-%d') -> Optional[List[Dict[str, str]]]:
    if is_null(start_date) and is_null(end_date):
        logging.error(msg='Invalid/Empty start or end date')
        return None

    if refresh_type != 'history':
        return [{'startDate': start_date, 'endDate': end_date}]

    start_date_time = cast_string_to_date(start_date, date_pattern)
    end_date_time = cast_string_to_date(end_date, date_pattern)

    if start_date_time > end_date_time:
        logging.error(msg='Invalid start & end date: End date cannot be greater than start date')
        return None

    dates_between_range = []
    next_date = start_date_time
    while next_date <= end_date_time:
        _map = dict()
        _map.update({'startDate': next_date.date().__str__()})
        next_end_date = next_date + timedelta(days=(interval_in_days - 1))
        _map.update({'endDate': next_end_date.date().__str__()})
        dates_between_range.append(_map)
        next_date = next_end_date + timedelta(days=1)
    return dates_between_range


def init_logging(log_time_stamp):
    config = read_yaml_get_dict(yaml_path=f"{get_project_root()}/main/src/resources/config/logging.yaml")
    config['handlers']['file']['filename'] = f"{get_project_root()}/logs/log-spark-submit-wrapper_{log_time_stamp}.log"
    logging.config.dictConfig(config)
    logging.debug(f'Logging initiated with properties: {config}')


def execute_bash(shared_data, sleep_time, cmd):
    process: Optional[subprocess.CompletedProcess]
    yarn_application_id = ""
    try:
        init_logging(log_time_stamp=shared_data['log_timestamp'])
        process = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f'Command execution completed in parallel process pool, pid = {os.getpid()}, command = {cmd}')
        yarn_application_id = re.findall(r"application_\d{13}_\d{4}", str(process.stderr))
        if len(yarn_application_id):
            yarn_application_id = yarn_application_id[0]
        time.sleep(sleep_time)
        return os.getpid(), int(process.returncode.__str__()), yarn_application_id, process.stdout, process.stderr
    except subprocess.CalledProcessError as ex:
        logging.debug(f'Error executing bash command: (process_id, return_code, spark_yarn_application_id, '
                      f'stdout, stderr) = '
                      f'({os.getpid()}, {yarn_application_id} {ex.returncode}, {ex.stdout}, {ex.stderr})')
        print_exception_details(message=f'Error executing process id = {os.getpid()} '
                                        f'bash command {cmd}, below error traceback', exception_object=ex)
        return os.getpid(), int(ex.returncode.__str__()), yarn_application_id, ex.stdout, ex.stderr
    except Exception as ex:
        print_exception_details(f'Error executing bash command, below error traceback', ex)


@audit_action(action='All processes in pool executed')
def create_multiprocess_pool(shared_data, command_list, sleep_time=0, max_parallel_jobs=5):
    try:
        process_manager = Manager()
        process_manager.Value('log_timestamp', shared_data['log_timestamp'])

        with concurrent.futures.ProcessPoolExecutor(max_workers=max_parallel_jobs) as executor:
            partial_run_bash_command = functools.partial(execute_bash, shared_data, sleep_time)
            logging.debug(f'Multiprocessing pool created with properties {executor.__dict__}')
            results = list(executor.map(partial_run_bash_command, command_list))
            failures = list(filter(lambda r: r[1] is None or r[1] > 0, [res for res in results]))
        if len(failures) > 0:
            logging.error(f"Multiprocess pool execution completed, some commands in pool failed, below "
                          f"list of (process_id, return_code, spark_yarn_application_id, stdout, stderr) {list(failures)}")
    except Exception as ex:
        print_exception_details(message='Error creating multiprocessing pool, error traceback below',
                                exception_object=ex)
    return results, failures
