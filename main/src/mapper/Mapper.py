import abc

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DataType, StructType, ArrayType


class IMapper:
    xpaths: dict
    nestedxmlviews: list

    @abc.abstractmethod
    def getDataframeSchema(self, df: DataFrame): DataFrame

    def createDDL(self, df: DataFrame, database, table, location): str

    def complexTypeIterator(self, viewname, viewpath, database, table, xpath, level, dtype: DataType, acc={},
                            xpaths=[]): (dict, list)

    def handleArrayType(self, viewname, viewpath, database, table, xpath, level, dtype: ArrayType, acc={}, xpaths=[]): (
        dict, list)

    def handleStructType(self, viewname, viewpath, database, table, xpath, level, dtype, acc={}, xpaths=[]): (
        dict, list)


class XmlMapper(IMapper):
    outerselects = []

    def __init__(self, sc: SparkSession):
        self.spark = sc
        return df.schema

    def createDDL(self, df: DataFrame, database, table, location):
        newline = '\n'
        ddl = str("")
        if database.__eq__(""):
            ddl = str(f"CREATE EXTERNAL TABLE {table} {newline}({newline}")
        else:
            ddl = str(f"CREATE EXTERNAL TABLE {database}.{table} {newline}({newline}")

        bigarraytypes: list[(str, str)] = None

        for field in df.schema.fields:
            if len(field.dataType.simpleString()) <= 100000:
                ddl = ddl + str(f"`{field.name}` {field.dataType.simpleString()},{newline}")
            else:
                print(f"Found big tag {field.name} skipping.. as the type definition exceeds more than value set in "
                      f"Ambari > Hive > Configs > Advanced > Custom hive-site hive.metastore.max.typename.length=100000")
                # bigarraytypes += list[(field.name, field.dataType.sql)]

        ddl = ddl.rstrip(',\n')

        ddl += f"{newline}) {newline}" \
               f"STORED AS PARQUET {newline}" \
               f"LOCATION {location};{newline};"

        return ddl

    def createViewsAndXpaths(self, df: DataFrame, database, table):
        views = {}
        views = self.complexTypeIterator(viewname="", viewpath="", database=database, table=table, xpath=table, level=0,
                                         dtype=df.schema, acc={}, xpaths=[])
        return self.nestedxmlviews, self.xpaths

    def handleStructType(self, viewname, viewpath, database, table, xpath, level, dtype, acc={}, xpaths=[]):
        query = ""
        if not str(viewname).__eq__(""):
            keynm = f"{table.replace('.', '_')}_{viewname.replace('.', '_')}"
            viewpath = f"{table}.{viewname}"
            query = f"SELECT v{level}.* FROM {table} t{level} " \
                    f"LATERAL VIEW INLINE(ARRAY(t{level}.`{viewname}`)) v{level}"
            acc.update({keynm: query})
            table = keynm

        structtype: StructType = dtype
        for field in structtype.fields:
            viewname = field.name
            viewpath = f"{table}.{viewname}"
            newxpath = f"{xpath.replace('.', '/')}/{viewname.replace('.', '/')}"
            self.complexTypeIterator(viewname=viewname, viewpath=viewpath, database=database, table=table,
                                     xpath=newxpath, level=level,
                                     dtype=field.dataType, acc=acc, xpaths=xpaths)

        return acc, xpaths

    def handleArrayType(self, viewname, viewpath, database, table, xpath, level, dtype: ArrayType, acc={}, xpaths=[]):
        query = ""
        if dtype.elementType.typeName().lower().__eq__("struct"):
            keynm = f"{table.replace('.', '_')}_{viewname.replace('.', '_')}"
            newxpath = f"{xpath.replace('.', '/')}/{viewname.replace('.', '/')}"
            query = f"SELECT v{level}.* FROM {table} t{level} " \
                    f"LATERAL VIEW INLINE(t{level}.`{viewname}`) v{level}"
            acc.update({keynm: query})
            self.handleStructType(viewname=viewname, viewpath=viewpath, database=database, table=table, xpath=xpath,
                                  level=level + 1,
                                  dtype=dtype.elementType, acc=acc, xpaths=xpaths)
        elif dtype.elementType.typeName().lower().__eq__("array"):
            print("Array of Arrays detected ->" + dtype.elementType.simpleString())
            newxpath = f"{xpath.replace('.', '/')}/{viewname.replace('.', '/')}"
            self.handleArrayType(viewname=viewname, viewpath=viewpath, database=database, table=table, xpath=xpath,
                                 level=level + 1,
                                 dtype=dtype.elementType, acc=acc, xpaths=xpaths)
        else:
            if not str(viewname).__eq__(""):
                keynm = f"{table.replace('.', '_')}_{viewname.replace('.', '_')}"
            else:
                keynm = table.replace('.', '_')
            query = f"SELECT v{level}.* FROM {table} t{level} " \
                    f"LATERAL VIEW EXPLODE(t{level}.`{viewname}`) v{level}"
            acc.update({keynm: query})
            self.complexTypeIterator(viewname=viewname, viewpath=viewpath, database=database, table=table, xpath=xpath,
                                     level=level,
                                     dtype=dtype.elementType, acc=acc, xpaths=xpaths)
        return acc, xpaths

    def complexTypeIterator(self, viewname, viewpath, database, table, xpath, level, dtype: DataType, acc={},
                            xpaths=[]):
        selectquery = "SELECT "
        if dtype.typeName().lower().__eq__("struct"):
            self.handleStructType(viewname=viewname, viewpath=viewpath, database=database, table=table, xpath=xpath,
                                  level=level,
                                  dtype=dtype, acc=acc, xpaths=xpaths)
        elif dtype.typeName().lower().__eq__("array"):
            self.handleArrayType(viewname=viewname, viewpath=viewpath, database=database, table=table, xpath=xpath,
                                 level=level,
                                 dtype=dtype, acc=acc, xpaths=xpaths)
        else:
            xpaths.append(xpath)
            self.xpaths = xpaths
            self.nestedxmlviews = acc
            return acc, xpaths
        return acc, xpaths

    def buildXmlSerdeDdl(self, database, table, xmlsourcelocation, xmlrowstarttag, xmlrowendtag):
        querieslist = []
        # querieslist.append(f"CREATE DATABASE IF NOT EXISTS {database}")
        querieslist.append(f"DROP TABLE IF EXISTS {database}.{table}")
        ddlhead = f"CREATE TABLE IF NOT EXISTS {database}.xmltable"
        ddlcols = list()
        serdeproperties = list()
        x = 1
        for i in self.xpaths:
            ddlclmnm = self.shortenNames(
                str(i).replace('xmltable/', f'{xmlrowendtag}/').replace('/xmlvaluetag', '').replace('@', '').replace(
                    '/', '_'))
            ddlcols.append(
                f"col_{ddlclmnm[-118:]} ARRAY<STRING>")
            if str(i).__contains__('xmlvaluetag') or str(i).__contains__('@'):
                serdeprpty = self.shortenNames(
                    str(i).replace('xmltable/', f'{xmlrowendtag}/').replace('/xmlvaluetag', '').replace('@',
                                                                                                        '').replace('/',
                                                                                                                    '_'))
                serdeprptyval = str(i).replace('xmltable/', f'{xmlrowendtag}/').replace('/xmlvaluetag', '/text()')
                print(serdeprptyval)
                serdeproperties.append(
                    f"\"column.xpath.col_{serdeprpty[-118:]}\"=\"/{serdeprptyval}\"")
            else:
                serdeprpty = self.shortenNames(
                    str(i).replace('xmltable/', f'{xmlrowendtag}/').replace('/xmlvaluetag', '').replace('@',
                                                                                                        '').replace('/',
                                                                                                                    '_'))
                serdeprptyval = str(i).replace('xmltable/', f'{xmlrowendtag}/')
                serdeproperties.append(
                    f"\"column.xpath.col_{serdeprpty[-118:]}\"=\"/{serdeprptyval}/text()\"")
            x = x + 1
        ddltail = f"ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe' WITH SERDEPROPERTIES ( {','.join(list(serdeproperties))} ) STORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat' LOCATION '/data/DEV/stage/epic/xmltable' TBLPROPERTIES ( \"xmlinput.start\"=\"<{xmlrowstarttag}\", \"xmlinput.end\"=\"</{xmlrowendtag}>\")"
        ddlbody = f"({','.join(list(ddlcols))})"
        finalddl = f"{ddlhead} {ddlbody} {ddltail}"

        print(finalddl)
        querieslist.append(finalddl)

        querieslist.append(f"LOAD DATA INPATH '{xmlsourcelocation}' OVERWRITE INTO TABLE {database}.{table}")

        columnnames = list(map(lambda
                                   x: f"REGEXP_REPLACE(REGEXP_REPLACE(CONCAT_WS(\";\",{str(x).split(' ')[0]}), \"<string>\", \"\"), \"</string>\",\"\") AS {str(x).split(' ')[0]}",
                               ddlcols))

        querieslist.append(f"SELECT {','.join(columnnames)} FROM {database}.{table}")
        return querieslist
