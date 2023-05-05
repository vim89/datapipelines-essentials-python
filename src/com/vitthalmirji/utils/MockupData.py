from random import Random
from typing import Optional, Any

from pyspark.sql.types import *


# ToDo Yet to complete Random data generation
class Maybe(object):
    def get_or_else(self, default):
        return self.value if isinstance(self, Just) else default


class Just(Maybe):
    def __init__(self, value):
        self.value = value


class Nothing(Maybe):
    pass


# Random data generators for Spark SQL DataTypes. These generators do not generate uniformly random
# values; instead, they're biased to return "interesting" values (such as maximum / minimum values)
# with higher probability.
class MockupData:
    # The conditional probability of a non-null value being drawn from a set of "interesting" values
    # instead of being chosen uniformly at random.
    PROBABILITY_OF_INTERESTING_VALUE: float = 0.5

    # The probability of the generated value being null
    PROBABILITY_OF_NULL: float = 0.1

    MAX_STR_LEN: int = 1024
    MAX_ARR_SIZE: int = 128
    MAX_MAP_SIZE: int = 128

    # Returns a randomly generated schema, based on the given accepted types.
    # @param numFields the number of fields in this schema
    # @param acceptedTypes types to draw from.
    def randomSchema(self, rand: Random, numFields: int, acceptedTypes: list[DataType]) -> StructType:
        structfields = []
        i = 0
        while i < numFields:
            dt = acceptedTypes[rand.randint(1, len(acceptedTypes))]
            structfields.append(StructField(f"col_{i}", dt, nullable=bool(rand.getrandbits(1))))
        return StructType(structfields)

    # Returns a function which generates random values for the given `DataType`, or `None` if no
    # random data generator is defined for that data type. The generated values will use an external
    # representation of the data type; for example, the random generator for `DateType` will return
    # instances of [[java.sql.Date]] and the generator for `StructType` will return a [[Row]].
    # For a `UserDefinedType` for a class X, an instance of class X is returned.
    # #@param dataType the type to generate values for
    # @param nullable whether null values should be generated
    # @param rand an optional random number generator
    # @return a function which can be called to generate random values.
    def forType(self, dataType: DataType, nullable: bool, rand: Random = Random()) -> Optional[Any]:
        return Optional[Any]()

    # Generates a random row for `schema`.
    def randomRow(self, rand: Random, schema: StructType) -> Row:
        fields = list(StructField)
        for f in schema.fields:
            if str(f.dataType).lower().__eq__("arraytype"):
                data = None
                if f.nullable and rand.random() <= self.PROBABILITY_OF_NULL:
                    data = None
                else:
                    arr = []
                    n = 1
                    i = 0
                    _f: ArrayType = f.dataType()
                    generator = self.forType(_f.elementType, f.nullable, rand)
                    assert (generator.isDefined, "Unsupported")
                    gen = generator.get
                    while i < n:
                        arr.append(gen)
                        i = i + 1
                    data = arr
                fields.append(data)
            elif str(f.dataType).lower().__eq__("structtype"):
                _f: StructType = f
                for c in _f:
                    fields.append(self.randomRow(rand, StructType(c.dataType())))
            else:
                generator = self.forType(f.dataType, f.nullable, rand)
                assert (generator.isDefined, "Unsupported")
                gen = generator.get
                fields.append(gen)
        return Row(*fields)

    # Returns a random nested schema. This will randomly generate structs and arrays drawn from
    # acceptedTypes.
    def randomNestedSchema(self, rand: Random, totalFields: int, acceptedTypes: list[DataType]) -> StructType:
        fields = []
        i = 0
        numFields = totalFields
        while numFields > 0:
            v = rand.randint(0, 3)
            if v is 0:
                # Simple type
                dt = acceptedTypes[rand.randint(0, len(acceptedTypes))]
                fields.append(StructField(f"col_{i}", dt, bool(rand.getrandbits(1))))
                numFields = -1
            elif v is 1:
                # Array
                dt = acceptedTypes[rand.randint(0, len(acceptedTypes))]
                fields.append(StructField(f"col_{i}", ArrayType(dt), bool(rand.getrandbits(1))))
                numFields = -1
            else:
                n = max(rand.randint(0, numFields), 1)
                nested = self.randomNestedSchema(rand, n, acceptedTypes)
                fields.append(StructField("col_" + i, nested, bool(rand.getrandbits(1))))
                numFields = numFields - n

            i = i + 1
        return StructType(fields)
