import unittest
from pyspark.sql import SparkSession
import etl
from pyspark.sql.types import *
from Dependencies.spark import start_spark, conf_read


class SomeUnitTest(unittest.TestCase):

        def test_column(self):

            spark1, message_prefix1, logging1 = start_spark(app_name='my_etl_job')

            df11 = spark1.read.csv("client_data/", header=True, inferSchema=True)

            result = df11.schema.names

            column_name = ['client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type']
            self.assertEqual(result, column_name)


if __name__ == '__main__':

    spark, message_prefix, logging = start_spark(app_name='my_etl_job')

    config = conf_read()

    df1, df2 = etl.read_file(spark, logging, config["dataset1"], config["dataset2"])

    # taking name of the country as the filter from config file
    df_output = etl.business_function(spark, logging, df1, df2, config["countryFilter"])

    etl.save_output(logging, df_output, config["test_file"])

    unittest.main()
