from pyspark.sql.types import *

from com.vitthalmirji.etl import ETL


class CColumn:
    def __init__(self, colname, coldatatype, pk, filterclause, udf="", udfargs=[], casttype="", aliasname=""):
        self.colname = colname
        self.coldatatype = coldatatype
        self.pk = pk
        self.udf = udf
        self.udfargs = udfargs
        self.aliasname = aliasname
        self.casttype = casttype
        self.filterclause = filterclause
        self.selectexpression = ""
        self.matchmetatype = {
            'tinyint': IntegerType(),
            'smallint': IntegerType(),
            'int': IntegerType(),
            'bigint': LongType(),
            'long': LongType(),
            'float': FloatType(),
            'double': DoubleType(),
            'boolean': BooleanType(),
            'string': StringType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'binary': BinaryType()
        }

    def applyUdf(self):
        if ETL.isNullOrEmpty(self.udf) is None and len(self.udfargs) is 0:
            # tempcol: pyspark.sql.column.Column = col(str(self.colname))
            # tempcol.cast(self.matchmetatype[self.casttype]).alias(self.aliasname)
            self.selectexpression = f"CAST({self.colname} AS {self.casttype}) AS {self.aliasname},"
        elif ETL.isNullOrEmpty(self.udf) is not None and len(self.udfargs) is 0:
            # tempcol = col(self.colname)
            # kwargs = {'field': tempcol}
            # udfFunc = getattr(ETL, f"udf{str(self.udf).title()}")
            # tempcol = udfFunc(tempcol)
            # tempcol = tempcol.cast(self.matchmetatype[self.casttype]).alias(self.aliasname)
            self.selectexpression = f"CAST({self.udf}({self.colname}) AS {self.casttype}) AS {self.aliasname},"
        elif ETL.isNullOrEmpty(self.udf) is not None and len(self.udfargs) is not 0:
            # tempcol = col(self.colname)
            # udfFunc = getattr(ETL, f"udf{str(self.udf).title()}")
            # tempcol = udfFunc(tempcol)
            # tempcol = tempcol.cast(self.matchmetatype[self.casttype]).alias(self.aliasname)
            self.selectexpression = f"CAST({self.udf}({self.colname}, {','.join(self.udfargs)}) AS {self.casttype}) AS {self.aliasname}"
        else:
            self.selectexpression = f"CAST({self.colname} AS {self.casttype}) AS {self.aliasname},"
        return self.selectexpression
