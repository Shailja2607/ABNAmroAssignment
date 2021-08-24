import unittest
from pyspark.sql import SparkSession


class SomeUnitTest(unittest.TestCase):

    def test_column(self):
        spark1 = SparkSession.builder.getOrCreate()

        df11 = spark1.read.csv("sparkAssignment/client_data/", header=True, inferSchema=True)

        result = df11.schema.names

        column_name = ['client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type']

        self.assertEqual(result, column_name)


if __name__ == '__main__':
    unittest.main()
