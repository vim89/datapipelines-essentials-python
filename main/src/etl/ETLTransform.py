import functools

import pyspark.sql.functions as SparkSQLFunctions
from pyspark.sql import DataFrame, SparkSession

from etl import ETL
from etl.ITable import SourceTable, TargetTable, matchEqualityOperator
from etl.meta.MetaModel import MetaModel, MetaResult


# ToDo - Source & Target group aggregations

class Transform:
    def __init__(self, targettable, model: MetaModel, sc: SparkSession):
        self.model = model
        self.spark = sc
        self.sourcetables: list[SourceTable] = []
        self.targettable = targettable
        self.transformquery = ""
        self.joindict = {}
        self.sourcetablesdf: list[DataFrame] = []
        self.targetdf: DataFrame = None
        self.targetcolumnslist = []
        self.joincolumns = None
        self.jointype = None

    def genericDfOperation(self, operationFunc):
        return operationFunc(self)

    DataFrame.genericDfOperation = genericDfOperation

    def filterSourceTable(self, srctbl):
        srctbls = filter(lambda tbl: tbl.tablename == srctbl, self.sourcetables)
        return list(srctbls)

    def joinDataframes(self, dict1, dict2):
        targetdf: DataFrame = dict1['df'].join(dict2['df'], on=dict2['condition'], how=dict2['jointype'])
        return {'df': targetdf}

    def mapAggregationFunction(self, fieldname, functionname):
        if str(functionname).__eq__('min'):
            return SparkSQLFunctions.min(col=SparkSQLFunctions.col(fieldname))
        elif str(functionname).__eq__('max'):
            return SparkSQLFunctions.max(col=SparkSQLFunctions.col(fieldname))
        elif str(functionname).__eq__('count'):
            return SparkSQLFunctions.count(col=SparkSQLFunctions.col(fieldname))
        elif str(functionname).__eq__('sum'):
            return SparkSQLFunctions.sum(col=SparkSQLFunctions.col(fieldname))
        elif str(functionname).__eq__('avg'):
            return SparkSQLFunctions.avg(col=SparkSQLFunctions.col(fieldname))

    def applyJoin(self):
        self.query, self.joindict = self.model.joinSQL(self.model.datamodel, 'purchase', 'product', 'store')

        joinlist = []
        for k in self.joindict.keys():
            srctabledf: DataFrame = self.filterSourceTable(k)[0].targetdf
            self.joindict[k].update({'df': srctabledf})
            joinlist.append(self.joindict[k])

        self.targetdf: DataFrame = functools.reduce(self.joinDataframes, joinlist)['df']

    def applyFilters(self):
        tblinfo: MetaResult = self.model.filterMetaResultBySourceTable(self.sourcetables[0].tablename)
        targettable: TargetTable = TargetTable(sourcesystem=tblinfo.src_system, tablename=tblinfo.target_table, pk=[],
                                               database=tblinfo.target_database,
                                               filetype=tblinfo.target_filetype, filepath=tblinfo.target_file_path,
                                               modeltableorder=tblinfo.src_table_order)

        for metares in self.model.metaresultlist:
            filterexpr = matchEqualityOperator(expression=metares.src_col_filter)
            if filterexpr is not None and not filterexpr.__eq__("") and not filterexpr.lower().__eq__('none'):
                self.filterclause = f"{self.filterclause} {metares.target_col}{filterexpr}".strip()

            self.filterclause = self.filterclause.strip()

            if self.filterclause is None:
                self.filterclause = ""

        targettable.df: DataFrame = self.targetdf.filter(self.filterclause)

    def applyGroupAndAggregation(self):
        selectlist = []
        aggregations = {}
        for metares in self.model.filterMetaResultByTargetTable(self.targettable):
            if ETL.isNullOrEmpty(metares.target_col_aggregator) is not None:
                selectlist.append(metares.target_col)
            else:
                aggregations.update({
                    metares.target_col: {
                        'function': metares.target_col_aggregator,
                        'filter': metares.target_col_aggregator_filter
                    }
                })

        self.targetdf: DataFrame = self.targetdf.groupby(*selectlist).agg(SparkSQLFunctions.min)

    def transform(self):
        # Get Unique source table names for Transformation
        srctables = set()
        for metares in self.model.metaresultlist:
            srctables.add(metares.src_table)

        # For each source table create SourceTable object and assign transform columns
        for srctable in srctables:
            tablemetaresult = self.model.filterMetaResultBySourceTable(srctbl=srctable)
            tblinfo: MetaResult = tablemetaresult[0]

            fklist = []

            for item in self.model.datamodel.keys():
                if self.model.datamodel[item]['fk'] is not None or self.model.datamodel[item]['fk'] is {}:
                    if srctable in self.model.datamodel[item]['fk'].keys():
                        fklist.extend(self.model.datamodel[item]['fk'][srctable]['fk_pk'])

            sourcetable: SourceTable = SourceTable(sourcesystem=tblinfo.src_system, tablename=tblinfo.src_table,
                                                   pk=self.model.datamodel[tblinfo.src_table]['pk'],
                                                   fk=fklist,
                                                   database=tblinfo.src_database, filepath=tblinfo.src_file_path,
                                                   filetype=tblinfo.src_filetype,
                                                   modeltableorder=tblinfo.src_table_order)
            self.sourcetables.append(sourcetable)
            for tbl in tablemetaresult:
                sourcetable.addColumn(name=tbl.src_col, type=tbl.src_col_datatype,
                                      pk=(True, False)[tbl.src_key_constraints.__eq__('pk')],
                                      udf=tbl.udf, udfargs=tbl.udfarguments, casttype=tbl.target_col_datatype,
                                      aliasname=tbl.target_col, filterclause=tbl.src_col_filter, fk={})

            # Read file as dataframe
            sourcetable.readFileFromSource(spark=self.spark)

        ETL.registerAllUDF(sc=self.spark)
        for sourcetable in self.sourcetables:
            sourcetable.applyTransform()

        self.applyJoin()

        self.applyFilters()

        self.applyGroupAggregation()

        self.targetdf.show()
