import abc
from abc import ABC

from pyspark.sql import DataFrame, SparkSession

from com.vitthalmirji.etl import ETL
from com.vitthalmirji.etl.CColumn import CColumn
from com.vitthalmirji.imports.HdfsImport import HdfsImport


class ITable:
    sourcesystem: str
    tablename: str
    columnlist: []
    pk: []
    fk: []
    database: str
    filepath: str
    modeltableorder: int

    @abc.abstractmethod
    def getColumnList(self): []

    @abc.abstractmethod
    def getPkList(self): []

    @abc.abstractmethod
    def getFkList(self): []

    @abc.abstractmethod
    def getPath(self): str

    @abc.abstractmethod
    def getDatabaseName(self): str

    @abc.abstractmethod
    def readFileFromSource(self, park: SparkSession, opt={}, tbl=""): DataFrame


def matchEqualityOperator(expression):
    expr = str(expression)
    if expr is None or expr.__eq__("None"):
        expr = str("")
    elif expr.find('eq(') != -1:
        expr = expr.replace('eq(', '=').replace(')', '')
    if expr.find('gt') != -1:
        expr = expr.replace('gt(', '>').replace(')', '')
    elif expr.find('lt') != -1:
        expr = expr.replace('lt(', '<').replace(')', '')
    elif expr.find('lte') != -1:
        expr = expr.replace('lte(', '<=').replace(')', '')
    elif expr.find('gte') != -1:
        expr = expr.replace('gte(', '>=').replace(')', '')
    elif expr.find('notin') != -1:
        expr = expr.replace('notin', 'NOT IN')
    elif expr.find('in') != -1:
        expr = expr.replace('in', 'IN')
    elif expr.find('ne') != -1:
        expr = expr.replace('ne(', '<>').replace(')', '')
    else:
        expr = expr.strip()

    if expr is None or expr.__eq__('None'):
        expr = ""

    return expr


class SourceTable(ITable):
    def __init__(self, sourcesystem, tablename, pk, fk, database, filetype, filepath, modeltableorder):
        self.tablename = tablename
        self.pk = pk
        self.fk = fk
        self.database = database
        self.sourcesystem = sourcesystem
        self.filepath = filepath
        self.filetype = filetype
        self.modeltableorder = modeltableorder
        self.df: DataFrame = None
        self.columnlist: list[CColumn] = []
        self.filterclause = ""

    def getFilterCondition(self):
        return self.filterclause

    def addColumn(self, name, type, pk, udf, udfargs, casttype, aliasname, filterclause, fk={}) -> None:
        col = CColumn(colname=name, coldatatype=type, pk=pk, udf=udf, udfargs=udfargs, casttype=casttype,
                      aliasname=aliasname, filterclause=filterclause)

        filterexpr = matchEqualityOperator(expression=filterclause)
        if filterexpr is not None and not filterexpr.__eq__("") and not filterexpr.lower().__eq__('none'):
            self.filterclause = f"{self.filterclause} {name}{filterexpr}".strip()

        self.filterclause = self.filterclause.strip()

        if self.filterclause is None:
            self.filterclause = ""

        self.columnlist.append(col)

    def getPkList(self) -> []:
        return self.pk

    def getFkList(self) -> []:
        return self.fk

    def getColumnList(self) -> []:
        return self.columnlist

    def getDatabaseName(self) -> str:
        return self.database

    def getPath(self) -> str:
        return self.filepath

    def readFileFromSource(self, spark: SparkSession, opt={}, tbl="") -> DataFrame:
        importModule = HdfsImport(spark=spark)
        sourcedf = importModule.readFromSource(location=self.filepath, filetype=self.filetype, opt=opt)
        self.df: DataFrame = sourcedf
        return sourcedf

    def getDf(self) -> DataFrame:
        return self.df

    def applyTransform(self):
        selectexpression = ""
        for _srccol in self.columnlist:
            srccol: CColumn = _srccol
            selectexpression = f"{selectexpression}{srccol.applyUdf()}"

        selectexpression = f"{selectexpression}--End"
        selectexpression = selectexpression.strip(',--End')

        for p in self.pk:
            selectexpression = f"{selectexpression}, {p} AS {self.tablename}{p}"

        for f in self.fk:
            selectexpression = f"{selectexpression}, {f}"

        if ETL.isNullOrEmpty(self.filterclause) is not None:
            self.targetdf: DataFrame = self.df.filter(self.filterclause).selectExpr(selectexpression)
        else:
            self.targetdf: DataFrame = self.df.selectExpr(selectexpression)

        return self.targetdf


class TargetTable(ITable, ABC):
    def __init__(self, sourcesystem, tablename, pk, database, filetype, filepath, modeltableorder):
        self.tablename = tablename
        self.pk = pk
        self.database = database
        self.sourcesystem = sourcesystem
        self.filepath = filepath
        self.filetype = filetype
        self.modeltableorder = modeltableorder
        self.df: DataFrame = None
        self.columnlist: list[CColumn] = []
        self.sourcetableslist = list[SourceTable] = []
        self.filterclause = ""
        self.aggregationcolumns = []
        self.aggregationfilter = []

    def getPkList(self) -> []:
        return self.pk

    def getFkList(self) -> []:
        return self.fk

    def getColumnList(self) -> []:
        return self.columnlist

    def getDatabaseName(self) -> str:
        return self.database

    def getPath(self) -> str:
        return self.filepath

    def addColumn(self, name, type, pk, filterclause) -> None:
        col = CColumn(colname=name, coldatatype=type, pk=pk, filterclause=filterclause)

        filterexpr = matchEqualityOperator(expression=filterclause)
        if filterexpr is not None and not filterexpr.__eq__("") and not filterexpr.lower().__eq__('none'):
            self.filterclause = f"{self.filterclause} {name}{filterexpr}".strip()

        self.filterclause = self.filterclause.strip()

        if self.filterclause is None:
            self.filterclause = ""

        self.columnlist.append(col)
