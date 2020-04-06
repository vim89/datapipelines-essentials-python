import io
import urllib
import zipfile
from collections import OrderedDict

from pyspark.sql import *
from tabulate import tabulate
from tqdm import tqdm

from objects.enums.Environments import Environment, Environments


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


# Reading zipped folder data in Pyspark filter by filename
def zip_extract_filter(x, filter_pattern, filter_condition):
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


# ToDo - Any other util functions if required
class SparkSettings(object):
    def __init__(self, appName):
        self.appName = appName
        self.environment: Environment = Environments().getEnvironmentByServer()

    def getSparkSession(self):
        try:
            # ============================
            # This is the part where spark object is created
            # ============================
            spark = SparkSession.builder \
                .appName(self.appName) \
                .enableHiveSupport() \
                .getOrCreate()
            # spark.conf.set("spark.executor.cores", no_of_cores)
            # spark.conf.set("spark.executor.memory", executor_memory)
            # spark.conf.set("spark.submit.deployMode", deploy_mode)
            spark.conf.set("spark.yarn.historyServer.address", self.environment.historyserver)
            spark.conf.set("spark.sql.crossJoin.enabled", "true")
            spark.conf.set("spark.sql.optimizer.maxIterations", "5000")
            spark.conf.set("spark.eventLog.dir", "hdfs:///spark2-history")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
            spark.conf.set("spark.shuffle.compress", "true")
            spark.conf.set("spark.io.compression.codec", "snappy")
            return spark
        except Exception:
            raise
