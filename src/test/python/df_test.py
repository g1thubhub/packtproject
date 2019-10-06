import unittest


class OurTests(unittest.TestCase):
    def runTest(self):
        from pyspark.sql.tests import ExamplePoint, ExamplePointUDT, DoubleType, StructType, StructField, Row
        row1 = (1.0, ExamplePoint(1.0, 2.0))
        row2 = (2.0, ExamplePoint(3.0, 4.0))
        schema = StructType([StructField("label", DoubleType(), False), StructField("point", ExamplePointUDT(), False)])
        df1 = self.spark.createDataFrame([row1], schema)
        df2 = self.spark.createDataFrame([row2], schema)

        result = df1.union(df2).orderBy("label").collect()
        self.assertEqual(result, [Row(label=1.0, point=ExamplePoint(1.0, 2.0)), Row(label=2.0, point=ExamplePoint(3.0, 4.0))])
