import csv
import operator
from collections import deque
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

from com.vitthalmirji.etl import ETL
from com.vitthalmirji.imports.HdfsImport import HdfsImport


class MetaResult:
    def __init__(self, src_system, src_database, src_table, src_filetype, src_file_path, src_col, src_col_datatype,
                 src_key_constraints,
                 src_col_filter,
                 src_col_aggregator,
                 src_col_aggregator_filter,
                 src_table_order,
                 target_database,
                 target_table,
                 target_filetype,
                 target_file_path, target_col, target_col_datatype, udf="", udfarguments=""):
        self.metacolumnslist = {}
        self.src_system = src_system
        self.src_database = src_database
        self.src_table = src_table
        self.src_filetype = src_filetype
        self.src_file_path = src_file_path
        self.src_col = src_col
        self.src_col_datatype = str(src_col_datatype).lower()
        self.src_key_constraints = str(src_key_constraints).lower()
        self.src_col_filter = src_col_filter
        self.src_col_aggregator = src_col_aggregator
        self.src_col_aggregator_filter = src_col_aggregator_filter
        self.src_table_order = int(str(src_table_order).strip())
        self.target_database = target_database
        self.target_table = target_table
        self.target_filetype = target_filetype
        self.target_file_path = target_file_path
        self.target_col = target_col
        self.target_col_datatype = target_col_datatype
        self.target_col_aggregator = ""
        self.target_col_aggregator_filter = ""
        self.udf = udf
        if ETL.isNullOrEmpty(udfarguments) is not None:
            self.udfarguments = udfarguments.split('|')
        else:
            self.udfarguments = []

        self.metacolumnslist.update({'src_filetype': self.src_filetype})
        self.metacolumnslist.update({'src_system': self.src_system})
        self.metacolumnslist.update({'src_database': self.src_database})
        self.metacolumnslist.update({'src_table': self.src_table})
        self.metacolumnslist.update({'src_file_path': self.src_table})
        self.metacolumnslist.update({'src_col': self.src_table})
        self.metacolumnslist.update({'src_col_datatype': self.src_col_datatype})
        self.metacolumnslist.update({'src_key_constraints': self.src_key_constraints})
        self.metacolumnslist.update({'src_col_filter': self.src_col_filter})
        self.metacolumnslist.update({'src_col_aggregator': self.src_col_aggregator})
        self.metacolumnslist.update({'src_col_aggregator_filter': self.src_col_aggregator_filter})
        self.metacolumnslist.update({'src_table_order': self.src_table_order})
        self.metacolumnslist.update({'target_database': self.target_database})
        self.metacolumnslist.update({'target_table': self.target_table})
        self.metacolumnslist.update({'target_filetype': self.target_filetype})
        self.metacolumnslist.update({'target_file_path': self.target_file_path})
        self.metacolumnslist.update({'target_col': self.target_col})
        self.metacolumnslist.update({'target_col_datatype': self.target_col_datatype})
        self.metacolumnslist.update({'target_col_aggregator': self.target_col_aggregator})
        self.metacolumnslist.update({'target_col_aggregator_filter': self.target_col_aggregator_filter})
        self.metacolumnslist.update({'udf': self.udf})
        self.metacolumnslist.update({'udfarguments': self.udfarguments})

    def getMetaColumnsList(self):
        return self.metacolumnslist


class MetaModel:
    def __init__(self, datamodelpath, sc: SparkSession):
        self.spark = sc
        self.datamodelpath = datamodelpath
        self.metaresultlist: list[MetaResult] = None
        self.metaframe: DataFrame = None
        self.src_tables = dict()
        self.datamodel = self.readDatamodelFromCsv(path=self.datamodelpath)
        self.source_tables_dataframes = {}
        self.matchmetatype = {
            'tinyint': IntegerType,
            'smallint': IntegerType,
            'int': IntegerType,
            'bigint': LongType,
            'long': LongType,
            'float': FloatType,
            'double': DoubleType,
            'boolean': BooleanType,
            'string': StringType,
            'date': DateType,
            'timestamp': TimestampType,
            'binary': BinaryType
        }

    def sortedMetaResults(self):
        return self.metaresultlist.sort(key=operator.attrgetter('src_table_order'))

    def create_df(self, rows_data, col_specs):
        struct_fields = list(map(lambda x: StructField(*x), col_specs))
        return self.createDataFrame(data=rows_data, schema=StructType(struct_fields))

    SparkSession.create_df = create_df

    def filterMetaResultBySourceTable(self, srctbl):
        srctbls = filter(lambda tbl: tbl.src_table == srctbl, self.metaresultlist)
        return list(srctbls)

    def filterMetaResultByTargetTable(self, targettbl):
        metaresults = filter(lambda tbl: tbl.target_table == targettbl, self.metaresultlist)
        return list(metaresults)

    def filterMetaResultByTargetTableAggregation(self, targettbl):
        metaresults = filter(
            lambda tbl: (tbl.target_table == targettbl and ETL.isNullOrEmpty(tbl.target_col_aggregator) is not None),
            self.metaresultlist)
        return list(metaresults)

    def filterMetaResultBySourceTableAncColumn(self, srctbl, srccol):
        srctbls = filter(lambda tbl: tbl.src_table == srctbl and tbl.src_col == srccol, self.metaresultlist)
        return list(srctbls)

    def joinSQL(self, schema, main_table, *tables):
        # for each table, which tables does it connect to?
        children_map = {table: set() for table in schema}
        for child, properties in schema.items():
            parents = properties['fk']
            for parent in parents:
                children_map[parent].add(child)

        # What are all the tables in consideration?
        nodes = set(schema.keys())

        # get a tree of parent tables via breadth-first search.
        parent_tree = transformBreadthFirstSearch(nodes, children_map, main_table)

        # Create a topological ordering on the graph;
        # order so that parent is joined before child.
        join_order = []
        used = {main_table}

        def addToJoinOrder(t):
            if t in used or t is None:
                return
            parent = parent_tree.get(t, None)
            addToJoinOrder(parent)
            join_order.append(t)
            used.add(t)

        for table in tables:
            addToJoinOrder(table)

        lines = [f"FROM {main_table}"]
        joindict = {main_table: {'table': main_table, 'condition': [], 'jointype': '', 'ismaintable': 'yes'}}
        for fk_table in join_order:
            parent_table = parent_tree[fk_table]
            parent_cols = schema[parent_table]['pk']
            fk_cols = schema[fk_table]['fk'][parent_table]['fk_pk']
            jointype = schema[fk_table]['fk'][parent_table]['jointype']
            joinline = f'{jointype} JOIN {fk_table} ON'
            cond = ""
            for (parent_col, fk_col) in zip(parent_cols, fk_cols):
                cond = f"{cond}{fk_table}.{parent_col} = {parent_table}.{fk_col} AND"
            cond = f"{cond}--End"
            cond = cond.strip('AND--End')
            lines.append(f'{joinline} {cond}')

            # Spark Dataframes
            joincols = [f"{fk_table}{parent_col}"]
            if fk_table in joindict.keys():
                joincols.extend(joindict[fk_table]['condition'])

            joininfo = {fk_table: {'table': fk_table, 'condition': joincols, 'jointype': jointype, 'ismaintable': 'no'}}
            joindict.update(joininfo)

        return "\n".join(lines), joindict

    def validateMetadata(self, metarow={}) -> (bool, str):
        msg = "%s field in metadata should not be empty"
        if ETL.isNullOrEmpty(metarow['src_table']) is None:
            return False, f"{msg % metarow['src_table']}"
        if ETL.isNullOrEmpty(metarow['src_col']) is None:
            return False, f"{msg % metarow['src_col']}"
        if ETL.isNullOrEmpty(metarow['src_file_path']) is None:
            return False, f"{msg % metarow['src_file_path']}"
        if ETL.isNullOrEmpty(metarow['src_filetype']) is None:
            return False, f"{msg % metarow['src_filetype']}"
        if ETL.isNullOrEmpty(metarow['target_table']) is None:
            return False, f"{msg % metarow['target_table']}"
        if ETL.isNullOrEmpty(metarow['target_col']) is None:
            return False, f"{msg % metarow['target_col']}"
        if ETL.isNullOrEmpty(metarow['target_col_datatype']) is None:
            return False, f"{msg % metarow['target_col_datatype']}"
        if ETL.isNullOrEmpty(metarow['target_file_path']) is None:
            return True, f"{msg % metarow['target_file_path']}"
        if ETL.isNullOrEmpty(metarow['udfarguments']) is not None and ETL.isNullOrEmpty(metarow['udf']) is None:
            return False, f"{msg}" % "If UDFArguments is empty then UDF"

        return True, "Metadata validation complete - All good"

    def readMetadataFromCsv(self, sc: SparkSession, metadatapath, targettable):
        metaresultlist: list[MetaResult] = []
        metaframe: DataFrame = None
        try:
            metaframe: DataFrame = sc.read.csv(path=metadatapath, sep=',', header=True, inferSchema=True)
            metaframe = metaframe.filter(f"target_table=='{targettable}'")
            self.metaframe = metaframe
        except Exception as ex:
            print(f"Error reading file {metadatapath} -> {ex}")

        for _row in metaframe.collect():
            row: Row = _row
            rowdict = row.asDict(True)
            validMetadata, metadataValidationMsg = self.validateMetadata(rowdict)
            if validMetadata is True:
                metaresult = MetaResult(
                    src_system=rowdict['src_system'],
                    src_database=rowdict['src_database'],
                    src_table=rowdict['src_table'],
                    src_filetype=rowdict['src_filetype'],
                    src_file_path=rowdict['src_file_path'],
                    src_col=rowdict['src_col'],
                    src_col_datatype=rowdict['src_col_datatype'],
                    src_key_constraints=rowdict['key_constraints'],
                    src_col_filter=rowdict['src_col_filter'],
                    src_col_aggregator=rowdict['src_col_aggregator'],
                    src_col_aggregator_filter=rowdict['src_col_aggregator_filter'],
                    src_table_order=rowdict['src_table_order'],
                    target_database=rowdict['target_database'],
                    target_table=rowdict['target_table'],
                    target_col=rowdict['target_col'],
                    target_col_datatype=rowdict['target_col_datatype'],
                    udf=rowdict['udf'],
                    udfarguments=rowdict['udfarguments'],
                    target_file_path=rowdict['target_file_path']
                )
                metaresultlist.append(metaresult)
            else:
                print(f"Error in metadata: {metadataValidationMsg}")
        self.metaresultlist = metaresultlist

    def readDatamodelFromCsv(self, path) -> {}:
        return self.mapCsv2MetadataDict(path)

    def readSourceFilesIntoDF(self):
        src_tables = dict()
        for metares in self.metaresultlist:
            src_tables.update({
                str(metares.src_table).lower(): {
                    'src_file_path': metares.src_file_path,
                    'src_filetype': metares.src_filetype
                }
            })

        importmodule = HdfsImport(self.spark)
        for item in src_tables:
            localdf = importmodule.readFromSource(location=str(src_tables[str(item)]['src_file_path']),
                                                  filetype=str(src_tables[str(item)]['src_filetype']))
            src_tables.update({
                str(item): {
                    'src_file_path': metares.src_file_path,
                    'src_filetype': metares.src_filetype,
                    'src_df': localdf
                }
            })

            localdf.createOrReplaceTempView(str(item).lower())

        self.source_tables_dataframes = src_tables

    def mapCsv2MetadataDict(self, csvpath) -> {}:
        metadata = {}
        with open(csvpath, 'r') as f:
            try:
                reader = csv.reader(f)
            except Exception as ex:
                print(f"Error reading csv file {csvpath} -> {ex}")
            next(reader, None)  # skip the headers
            for row in reader:
                if row[0] in metadata and not str(row[2]).strip().__eq__("") and not str(row[3]).strip().__eq__(""):
                    metadata[row[0]]['fk'].update(
                        {row[2]: {'fk_pk': str(row[3]).strip().split(';'), 'jointype': str(row[4]).strip().upper()}})
                else:
                    if not row[2] == "" and not row[3] == "":
                        metadata.update({row[0]: {'pk': str(row[1]).strip().split('|'),
                                                  'fk': {row[2]: {'fk_pk': str(row[3]).strip().split(';'),
                                                                  'jointype': str(row[4]).strip().upper()}}}})
                    else:
                        metadata.update({row[0]: {'pk': str(row[1]).strip().split('|'), 'fk': {}}})
        return metadata

    def applyColTransform(self, query, src_table, src_col, target_col, target_col_datatype, udf, udfarguments):
        if ETL.isNullOrEmpty(udf) is not None and len(udfarguments) is not 0:
            query = f"{query} CAST({udf}({src_table}.`{src_col}`, {','.join(udfarguments)}) AS {target_col_datatype}) AS {target_col},"
        elif ETL.isNullOrEmpty(udf) is not None and len(udfarguments) is 0:
            query = f"{query} CAST({udf}({src_table}.`{src_col}`) AS {target_col_datatype}) AS {target_col},"
        else:
            query = f"{query} CAST({src_table}.`{src_col}` AS {target_col_datatype}) AS {target_col},"

        return query

    def getTransformSql(self):
        query = ""
        for metares in self.metaresultlist:
            query = self.applyColTransform(
                query=query,
                src_table=metares.src_table,
                src_col=metares.src_col,
                target_col=metares.target_col,
                target_col_datatype=metares.target_col_datatype,
                udf=metares.udf,
                udfarguments=metares.udfarguments
            )
        query = f"{query}--End"
        query = query.strip(',--End')
        return query

    def matchEqualityOperator(self, expression):
        expr = str(expression).strip()
        if expr is None:
            expr = ""
        elif expr.__contains__('eq'):
            expr = expr.replace('eq(', '=').replace(')', '', 1)
        elif expr.__contains__('gt'):
            expr = expr.replace('gt(', '>').replace(')', '', 1)
        elif expr.__contains__('lt'):
            expr = expr.replace('lt(', '<').replace(')', '', 1)
        elif expr.__contains__('lte'):
            expr = expr.replace('lte(', '<=').replace(')', '', 1)
        elif expr.__contains__('gte'):
            expr = expr.replace('gte(', '>=').replace(')', '', 1)
        elif expr.__contains__('notin'):
            expr = expr.replace('notin(', 'NOT IN').replace(')', '', 1)
        elif expr.__contains__('in'):
            expr = expr.replace('in(', 'IN').replace(')', '', 1)
        elif expr.__contains__('ne'):
            expr = expr.replace('ne(', '<>').replace(')', '', 1)
        else:
            expr = str(expression).strip()
        return expr

    def getWhereClauses(self):
        def matchEqualityOperator(expression):
            expr = str(expression).strip()
            if expr.__contains__('eq'):
                expr = expr.replace('eq(', '=').replace(')', '', 1)
            if expr.__contains__('gt'):
                expr = expr.replace('gt(', '>').replace(')', '', 1)
            elif expr.__contains__('lt'):
                expr = expr.replace('lt(', '<').replace(')', '', 1)
            elif expr.__contains__('lte'):
                expr = expr.replace('lte(', '<=').replace(')', '', 1)
            elif expr.__contains__('gte'):
                expr = expr.replace('gte(', '>=').replace(')', '', 1)
            elif expr.__contains__('notin'):
                expr = expr.replace('notin(', 'NOT IN').replace(')', '', 1)
            elif expr.__contains__('in'):
                expr = expr.replace('in(', 'IN').replace(')', '', 1)
            elif expr.__contains__('ne'):
                expr = expr.replace('ne(', '<>').replace(')', '', 1)
            else:
                expr = str(expression).strip()
            return expr

        query, joindict = self.joinSQL(self.datamodel, 'purchase', 'product', 'store')

        wherequery = ""
        for metares in self.metaresultlist:
            if str(metares.src_col_filter).strip() is not None and not str(metares.src_col_filter).strip().__eq__(""):
                wherequery = f"{metares.src_table}.`{metares.src_col}` {self.matchEqualityOperator(metares.src_col_filter)}"
        if ETL.isNullOrEmpty(wherequery) is not None:
            query = f"{query} WHERE {wherequery}"
        return query

    def getGroupAndAggregations(self, _query):
        query = _query
        tgtcols = []
        srccols = []
        tgttbl = "grp_" + str(self.metaresultlist[0].target_table).strip().lower()
        for metares in self.metaresultlist:
            if not str(metares.src_col_aggregator).lower().__eq__("'true'"):
                srccols.append(f"{metares.src_table}.`{str(metares.src_col).strip()}`")

            tgtcols.append(str(metares.target_col).strip())
        query = f"{query} GROUP BY {','.join(srccols)}"

        for metares in self.metaresultlist:
            if str(metares.src_col_aggregator).strip().lower().__eq__("'true'"):
                query = f"{query} HAVING {metares.udf}(*){self.matchEqualityOperator(metares.src_col_aggregator_filter)}"

        query = f"{','.join(tgtcols)} FROM ( SELECT {query} ) {tgttbl}"

        return query

    def getTargetDdl(self, tableformat, external=True, _ddl=""):
        ddl = _ddl
        databasename = self.metaresultlist[0].target_database
        tablename = self.metaresultlist[0].target_table
        tablelocation = self.metaresultlist[0].target_file_path
        if external is True and ETL.isNullOrEmpty(databasename) is None:
            ddl = f"CREATE EXTERNAL TABLE {tablename}(\n"
        elif external is True and ETL.isNullOrEmpty(databasename) is not None:
            ddl = f"CREATE EXTERNAL TABLE {databasename}.{tablename}(\n"
        elif external is False and ETL.isNullOrEmpty(databasename) is None:
            ddl = f"CREATE TABLE {tablename}(\n"
        elif external is False and ETL.isNullOrEmpty(databasename) is not None:
            ddl = f"CREATE TABLE {databasename}.{tablename}(\n"
        else:
            ddl = f"CREATE EXTERNAL TABLE {databasename}.{tablename}(\n"

        for metares in self.metaresultlist:
            if int(str(metares.src_table_order).strip()).__eq__(0):
                ddl = f"{ddl}`{metares.target_col}` {metares.target_col_datatype},\n"

        ddl = f"{ddl}--End"
        ddl = ddl.strip(',\n--End')
        ddl = f"{ddl}\n)" \
              f"STORED AS {tableformat}"
        if ETL.isNullOrEmpty(tablelocation) is not None and external is True:
            ddl = f"{ddl}\nLOCATION {tablelocation}"
        return ddl

    def getSourceDdl(self, databasename, tablename, tableformat, tablelocation, external=True, _ddl=""):
        ddl = _ddl
        if external is True and ETL.isNullOrEmpty(databasename) is None:
            ddl = f"CREATE EXTERNAL TABLE {tablename}(\n"
        elif external is True and ETL.isNullOrEmpty(databasename) is not None:
            ddl = f"CREATE EXTERNAL TABLE {databasename}.{tablename}(\n"
        elif external is False and ETL.isNullOrEmpty(databasename) is None:
            ddl = f"CREATE TABLE {tablename}(\n"
        elif external is False and ETL.isNullOrEmpty(databasename) is not None:
            ddl = f"CREATE TABLE {databasename}.{tablename}(\n"
        else:
            ddl = f"CREATE EXTERNAL TABLE {databasename}.{tablename}(\n"

        for metares in self.metaresultlist:
            ddl = f"{ddl}`{metares.src_col}` {metares.src_col_datatype},"

        ddl = f"{ddl}--End"
        ddl = ddl.strip(',--End')
        ddl = f"{ddl}\n)" \
              f"STORED AS {tableformat}"
        if ETL.isNullOrEmpty(tablelocation) is not None and external is True:
            ddl = f"{ddl}\nLOCATION {tablelocation}"
        return ddl

    def getTablesAndColsAsMap(self):
        return self.src_tables, self.src_cols


def transformBreadthFirstSearch(nodes, children, start):
    parent = {}
    q = deque([start])

    while q:
        v = q.popleft()
        for w in children[v]:
            if w not in parent:
                parent[w] = v
                q.append(w)

    return parent


class DataFrameMissingColumnError(ValueError):
    """raise this when there's a DataFrame column error"""


class DataFrameMissingStructFieldError(ValueError):
    """raise this when there's a DataFrame column error"""


class DataFrameProhibitedColumnError(ValueError):
    """raise this when a DataFrame includes prohibited columns"""


def applyTransformation(self, transformFunc):
    return transformFunc(self)


DataFrame.applyTransformation = applyTransformation


def validatePresenceOfColumns(df, required_col_names):
    all_col_names = df.columns
    missing_col_names = [x for x in required_col_names if x not in all_col_names]
    error_message = f"The {missing_col_names} columns are not included in the DataFrame with the following columns {all_col_names}"
    if missing_col_names:
        raise DataFrameMissingColumnError(error_message)


def validateSchema(df, required_schema):
    all_struct_fields = df.schema
    missing_struct_fields = [x for x in required_schema if x not in all_struct_fields]
    error_message = f"The {missing_struct_fields} StructFields are not included in the DataFrame with the following StructFields {all_struct_fields}"
    if missing_struct_fields:
        raise DataFrameMissingStructFieldError(error_message)


def validateAbsenseOfColumns(df, prohibited_col_names):
    all_col_names = df.columns
    extra_col_names = [x for x in all_col_names if x in prohibited_col_names]
    error_message = f"The {extra_col_names} columns are not allowed to be included in the DataFrame with the following columns {all_col_names}"
    if extra_col_names:
        raise DataFrameProhibitedColumnError(error_message)


def columnToList(df: DataFrame, col_name):
    return [x[col_name] for x in df.select(col_name).collect()]


def twoColumns2Dictionary(df: DataFrame, key_col_name, value_col_name):
    k, v = key_col_name, value_col_name
    return {x[k]: x[v] for x in df.select(k, v).collect()}


def toListOfDictionaries(df: DataFrame):
    return list(map(lambda r: r.asDict(), df.collect()))


class ColumnMismatchError(Exception):
    """raise this when there's a DataFrame column error"""


def assertColumnQuality(df, col_name1, col_name2):
    list1 = df.select(col_name1).collect()
    list2 = df.select(col_name2).collect()

    if list1 == list2:
        return True
    else:
        error_message = "The values of {col_name1} column are different from {col_name2}".format(
            col_name1=col_name1,
            col_name2=col_name2
        )
        print(list1)
        print(list2)
        raise ColumnMismatchError(error_message)


def snakeCaseColumnNames(df: DataFrame):
    return reduce(
        lambda memo_df, col_name: memo_df.withColumnRenamed(col_name, toSnakeCase()),
        df.columns,
        df
    )


def toSnakeCase(s):
    return s.lower().replace(" ", "_")


def sort_columns(df: DataFrame, sort_order):
    sorted_col_names = None
    if sort_order == "asc":
        sorted_col_names = sorted(df.columns)
    elif sort_order == "desc":
        sorted_col_names = sorted(df.columns, reverse=True)
    else:
        raise ValueError(
            f"['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'")
    return df.select(*sorted_col_names)
