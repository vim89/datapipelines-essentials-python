import time
import unittest

from main.src.imports.HdfsImport import HdfsImport
from main.src.mapper.Mapper import XmlMapper
from main.src.utils.Utilities import SparkSettings

start_time = time.time()


class XmlMapperTest(unittest.TestCase):
    def test2_CreateDDL_Views(self):
        print("Testing HdfsImport readFromSource")
        self.sparksettings = SparkSettings("XmlMapperTest")
        self.spark = self.sparksettings.getSparkSession()
        self.hdfsImport = HdfsImport(self.spark)
        self.hdfsImport.table = "test"
        self.hdfsImport.system = "syst"
        self.dataframe: DataFrame = self.hdfsImport.readFromSource(location='resources/EPIC_HWS_110919.xml',
                                                                   filetype='xml')

        self.txtdf: DataFrame = self.hdfsImport.readFromSource(location='resources/EPIC_HWS_110919.xml',
                                                               filetype='text')

        self.xmlmapper = XmlMapper(sc=self.spark)
        self.dataframe.printSchema()
        print(self.dataframe.count())

        self.dataframe.createOrReplaceTempView("xmltable")
        self.txtdf.createOrReplaceTempView('xmltabletext')
        self.assertGreater(self.dataframe.count(), 0)
        ddlstr = self.xmlmapper.createDDL(df=self.dataframe, database="", table="xmltable",
                                          location='/data/DEV/source/xmlfile')
        print(ddlstr)

        views, xpaths = self.xmlmapper.createViewsAndXpaths(df=self.dataframe, database="epic", table="xmltable")

        querieslist = self.xmlmapper.buildXmlSerdeDdl(database="epic", table="xmltable",
                                                      xmlsourcelocation='/data/DEV/EPIC_HWS_110919.xml',
                                                      xmlrowstarttag='HotelDescriptiveContent AreaUnitOfMeasureCode',
                                                      xmlrowendtag='HotelDescriptiveContent')

        print(querieslist)

        datalookupdf = self.hdfsImport.readFromSource(location='resources/OTALookup.csv', filetype='csv')
        datalookupdict = {}

        datalookupdf.show()
        for r in datalookupdf.collect():
            rowdict = r.asDict()
            if str(rowdict['NeedCode']).lower().__eq__('yes'):
                datalookupdict.update({f"{str(rowdict['Columns'])}": rowdict['OTALookup']})
            else:
                datalookupdict.update({f"{str(rowdict['Columns'])}": ''})

        lookupdf = self.hdfsImport.readFromSource(location='resources/OTALookupValues.csv', filetype='csv')
        lookupdf.show()
        lookupdict = {}
        for r in lookupdf.collect():
            rowdict = r.asDict()
            d = {
                f"{str(rowdict['OTALookup'])} {str(rowdict['OTALookupCode'])}": rowdict['OTALookupValue']
            }
            lookupdict.update(d)

        for q in querieslist:
            print(q)
            self.spark.sql(q).show()

        finaldf = self.spark.sql(querieslist[-1])

        finaldf.write.mode('overwrite').csv('Epic1G', header=True)

        ETL.lookup = lookupdict

        finalselcols = []
        for d in datalookupdict:
            if str(d) in finaldf.schema.names:
                if datalookupdict[d] is not None and not datalookupdict[d].__eq__(''):
                    finalselcols.append(f"udflookup('{datalookupdict[d]}', {str(d)}) AS {str(d)}")
                else:
                    finalselcols.append(f"{str(d)} AS {str(d)}")

        print(finalselcols)

        ETL.registerAllUDF(self.spark)
        finaldf.selectExpr(*finalselcols).coalesce(1).write.mode('overwrite').csv('finaxml1', header=True)


if __name__ == '__main__':
    unittest.main()
