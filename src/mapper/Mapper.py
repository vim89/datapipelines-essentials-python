import abc

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DataType, StructType, ArrayType


class IMapper:
    @abc.abstractmethod
    def getDataframeSchema(self, df: DataFrame): DataFrame

    def createDDL(self, df: DataFrame, database, table, location): str


class ComplexDataMapper(IMapper):
    outerselects = []

    def __init__(self, sc):
        self.spark: SparkSession = sc

    def getDataframeSchema(self, df: DataFrame) -> StructType:
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

    def createViews(self, df: DataFrame) -> {}:
        views = {}
        views, xpaths = self.complexTypeIterator(viewname="", viewpath="", database="",
                                                 table='xmltable', level=0,
                                                 dtype=df.schema, acc={}, root_table_name='xmltable')
        return views, xpaths

    def handleStructType(self, viewname, viewpath, database, table, level, dtype, acc={}, xpath=[]) -> {}:
        structtype: StructType = dtype
        selcols = []
        if viewname is None or str(viewname).__eq__(""):
            viewname = table
        for field in structtype.fields:
            if str(field.dataType).lower().startswith("struct"):
                selcols.append(f"t{level}.`{field.name}`")
                viewname = field.name
                viewpath = f"{table.replace('.', '_')}.{viewname.replace('.', '_')}"
                query = f"SELECT t{level}.`{field.name}`.*, t{level}.surrogate_id_{table}, ROW_NUMBER() OVER (ORDER BY " \
                        f"current_timestamp) AS surrogate_id_{viewpath.replace('.', '_')} FROM {table} t{level} "
                keynm = f"{table.replace('.', '_')}_{viewname}"
                acc.update({keynm: query})
                self.handleStructType(viewname=viewname, viewpath=viewpath, database=database, table=keynm, level=level,
                                      dtype=field.dataType, acc=acc)
            elif str(field.dataType).lower().startswith("array"):
                selcols.append(f"t{level}.`{field.name}`")
                arrtype: ArrayType = field.dataType
                if str(arrtype.elementType).lower().startswith("struct"):
                    viewname = field.name
                    viewpath = f"{table.replace('.', '_')}.{viewname.replace('.', '_')}"
                    query = f"SELECT v{level}.*, t{level}.surrogate_id_{table}, ROW_NUMBER() OVER (ORDER BY current_timestamp) AS surrogate_id_{viewpath.replace('.', '_')} FROM {table} t{level} LATERAL VIEW INLINE(t{level}.`{field.name}`) v{level}"
                    keynm = f"{table.replace('.', '_')}_{viewname}"
                    acc.update({keynm: query})
                    self.handleStructType(viewname=viewname, viewpath=viewpath, database=database, table=keynm,
                                          level=level + 1, dtype=arrtype.elementType, acc=acc)
                else:
                    viewname = field.name
                    viewpath = f"{table.replace('.', '_')}.{viewname.replace('.', '_')}"
                    query = f"SELECT v{level}.col AS {viewname}, t{level}.surrogate_id_{table}, ROW_NUMBER() OVER (ORDER BY " \
                            f"current_timestamp) AS surrogate_id_{viewpath.replace('.', '_')} FROM {table} t{level} " \
                            f"LATERAL VIEW EXPLODE(t{level}.`{field.name}`) v{level}"
                    keynm = f"{table.replace('.', '_')}_{viewname}"
                    acc.update({keynm: query})
                    xpath.append(f'{viewpath.replace(".", "/")}/{field.name}')
            else:
                xpath.append(f'{viewpath.replace(".", "/")}/{field.name}')
                selcols.append(f"t{level}.`{field.name}`")

            if len(selcols) > 0:
                query = f"SELECT {','.join(selcols)}, ROW_NUMBER() OVER (ORDER BY current_timestamp) AS surrogate_id_{table} FROM {table} t{level}"
                keynm = f"{table.replace('.', '_')}_{viewname}_outer"
                # acc.update({keynm: query})
        return acc

    def handleArrayType(self, viewname, viewpath, database, table, level, dtype: ArrayType, acc={}, xpath=[]) -> {}:
        if str(dtype.elementType).lower().startswith("struct"):
            arr_struct_type: StructType = dtype.elementType
            viewname = arr_struct_type.name
            viewpath = f"{table.replace('.', '_')}.{viewname.replace('.', '_')}"
            query = f"SELECT v{level}.*, t{level}.surrogate_id_{table}, ROW_NUMBER() OVER (ORDER BY current_timestamp) AS " \
                    f"surrogate_id_{viewpath.replace('.', '_')} FROM {table} t{level} " \
                    f"LATERAL VIEW INLINE(t{level}.`{arr_struct_type.name}`) v{level}"
            keynm = f"{table.replace('.', '_')}_{viewname}"
            acc.update({keynm: query})
            self.handleStructType(viewname=viewname, viewpath=viewpath, database=database, table=keynm,
                                  level=level + 1, dtype=arr_struct_type, acc=acc)
        else:
            viewname = viewname
            viewpath = viewpath
            query = f"SELECT v{level}.col AS {viewname}, t{level}.surrogate_id_{table}, ROW_NUMBER() OVER (ORDER BY " \
                    f"current_timestamp) AS surrogate_id_{viewpath.replace('.', '_')} FROM {table} t{level} " \
                    f"LATERAL VIEW EXPLODE(t{level}.`{viewname}`) v{level}"
            keynm = f"{table.replace('.', '_')}_{viewname}"
            acc.update({keynm: query})
            xpath.append(f'{viewpath.replace(".", "/")}/{viewname}')
        return acc, xpath

    def complexTypeIterator(self, viewname, viewpath, database, table, level,
                            dtype: DataType, acc={}, xpath=[], root_table_name='xmltable') -> {}:
        if viewname is None or str(viewname).__eq__(""):
            keynm = f"{table.replace('.', '_')}"
            query = f"SELECT t{level}.*, ROW_NUMBER() OVER (ORDER BY current_timestamp) AS surrogate_id_{table} FROM {root_table_name} t{level}"
            acc.update({keynm: query})
            table = keynm

        if dtype.typeName().lower().__eq__("struct"):
            self.handleStructType(viewname=viewname, viewpath=viewpath, database=database, table=table, level=level,
                                  dtype=dtype, acc=acc, xpath=[])
        elif dtype.typeName().lower().__eq__("array"):
            self.handleArrayType(viewname=viewname, viewpath=viewpath, database=database, table=table, level=level,
                                 dtype=dtype, acc=acc, xpath=[])
        else:
            xpath.append(f'{viewpath.replace(".", "/")}/{viewname}')
            return acc, xpath
        return acc, xpath
