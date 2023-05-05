import datetime

import pyspark.sql.functions as f
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import *

lookup = {}


# ToDo - Yet to add many potential UDFs

def registerAllUDF(sc: SparkSession):
    sc.udf.register(name='datetimetogmt', f=datetimeToGMT)
    sc.udf.register(name='zonedatetimetogmt', f=zoneDatetimeToGMTZone)
    sc.udf.register(name='isnullorempty', f=isNullOrEmpty)
    sc.udf.register(name='datetimetogmt', f=datetimeToGMT)
    sc.udf.register(name='udfnvl', f=udfNvl)
    sc.udf.register(name='udflookup', f=udfLookups)


def datetimeToGMT(dt, fmt):
    local = pytz.timezone("America/Los_Angeles")
    # format = "%Y-%m-%d %H:%M:%S"
    naive = datetime.datetime.strptime(str(dt).strip(), str(fmt).strip())
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt


def strSplitSep(s, sep=','):
    return str(s).split(str(sep))


def varargsToList(*fields, sep):
    return str(sep).join(fields)


def zoneDatetimeToGMTZone(dt, fmt, zone):
    local = pytz.timezone(str(zone).strip())
    # format = "%Y-%m-%d %H:%M:%S"
    naive = datetime.datetime.strptime(str(dt).strip(), str(fmt).strip())
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt


@f.udf(returnType=StringType())
def udfNvl(field):
    if isNullOrEmpty(field) is None:
        return "-"
    else:
        return field


@f.udf(returnType=StringType())
def udfLookups(clname, s):
    finallookupvalue = []
    if s is None:
        return ""
    else:
        codes = str(s).split(sep=';')
        for cd in codes:
            if f"{clname} {cd}" in lookup.keys():
                finallookupvalue.append(lookup[f"{clname} {cd}"])
            else:
                finallookupvalue.append(cd)

    return ';'.join(finallookupvalue)


def squared_udf(s):
    if s is None:
        return None
    return s * s


def nullString(s):
    return s is None or str(s).strip().__eq__("") is None


def isNullOrEmpty(s):
    if s is None:
        return None
    if str(s).strip() is None or str(s).strip().__eq__(""):
        return None
    return str(s).strip()
